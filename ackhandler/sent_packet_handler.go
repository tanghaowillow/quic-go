package ackhandler

import (
	"errors"
	"time"

	"github.com/lucas-clemente/quic-go/ackhandlerlegacy"
	"github.com/lucas-clemente/quic-go/congestion"
	"github.com/lucas-clemente/quic-go/frames"
	"github.com/lucas-clemente/quic-go/protocol"
	"github.com/lucas-clemente/quic-go/qerr"
	"github.com/lucas-clemente/quic-go/utils"
)

var (
	// ErrDuplicateOrOutOfOrderAck occurs when a duplicate or an out-of-order ACK is received
	ErrDuplicateOrOutOfOrderAck = errors.New("SentPacketHandler: Duplicate or out-of-order ACK")
	// ErrTooManyTrackedSentPackets occurs when the sentPacketHandler has to keep track of too many packets
	ErrTooManyTrackedSentPackets = errors.New("Too many outstanding non-acked and non-retransmitted packets")
	// ErrAckForSkippedPacket occurs when the client sent an ACK for a packet number that we intentionally skipped
	ErrAckForSkippedPacket = qerr.Error(qerr.InvalidAckData, "Received an ACK for a skipped packet number")
	errAckForUnsentPacket  = qerr.Error(qerr.InvalidAckData, "Received ACK for an unsent package")
)

var errPacketNumberNotIncreasing = errors.New("Already sent a packet with a higher packet number.")

type sentPacketHandler struct {
	lastSentPacketNumber protocol.PacketNumber
	lastSentPacketTime   time.Time
	skippedPackets       []protocol.PacketNumber

	LargestInOrderAcked protocol.PacketNumber
	LargestAcked        protocol.PacketNumber

	largestReceivedPacketWithAck protocol.PacketNumber

	packetHistory      *ackhandlerlegacy.PacketList
	stopWaitingManager stopWaitingManager

	retransmissionQueue []*ackhandlerlegacy.Packet

	bytesInFlight protocol.ByteCount

	rttStats   *congestion.RTTStats
	congestion congestion.SendAlgorithm
}

// NewSentPacketHandler creates a new sentPacketHandler
func NewSentPacketHandler() SentPacketHandler {
	rttStats := &congestion.RTTStats{}

	congestion := congestion.NewCubicSender(
		congestion.DefaultClock{},
		rttStats,
		false, /* don't use reno since chromium doesn't (why?) */
		protocol.InitialCongestionWindow,
		protocol.DefaultMaxCongestionWindow,
	)

	return &sentPacketHandler{
		packetHistory:      ackhandlerlegacy.NewPacketList(),
		stopWaitingManager: stopWaitingManager{},
		rttStats:           rttStats,
		congestion:         congestion,
	}
}

func (h *sentPacketHandler) ackPacket(packetElement *ackhandlerlegacy.PacketElement) *ackhandlerlegacy.Packet {
	packet := &packetElement.Value

	h.bytesInFlight -= packet.Length

	if h.LargestInOrderAcked == packet.PacketNumber-1 {
		h.LargestInOrderAcked++

		if next := packetElement.Next(); next != nil {
			h.LargestInOrderAcked = next.Value.PacketNumber - 1
		}
	}

	h.packetHistory.Remove(packetElement)

	return packet
}

func (h *sentPacketHandler) nackPacket(packetElement *ackhandlerlegacy.PacketElement) (*ackhandlerlegacy.Packet, error) {
	packet := &packetElement.Value

	packet.MissingReports++

	if packet.MissingReports > protocol.RetransmissionThreshold {
		h.queuePacketForRetransmission(packetElement)
		return packet, nil
	}
	return nil, nil
}

// does NOT set packet.Retransmitted. This variable is not needed anymore
func (h *sentPacketHandler) queuePacketForRetransmission(packetElement *ackhandlerlegacy.PacketElement) {
	packet := &packetElement.Value
	utils.Debugf("\tQueueing packet 0x%x for retransmission", packet.PacketNumber)
	h.bytesInFlight -= packet.Length
	h.retransmissionQueue = append(h.retransmissionQueue, packet)

	// If this is the lowest packet that hasn't been acked or retransmitted yet ...
	if packet.PacketNumber == h.LargestInOrderAcked+1 {
		// ... increase the LargestInOrderAcked until it's one before the next packet that was not acked and not retransmitted
		for el := packetElement; el != nil; el = el.Next() {
			if h.LargestInOrderAcked == h.LargestAcked {
				break
			}
			h.LargestInOrderAcked = el.Value.PacketNumber - 1
		}
	}

	h.packetHistory.Remove(packetElement)

	// strictly speaking, this is only necessary for RTO retransmissions
	// this is because FastRetransmissions are triggered by missing ranges in ACKs, and then the LargestAcked will already be higher than the packet number of the retransmitted packet
	h.stopWaitingManager.QueuedRetransmissionForPacketNumber(packet.PacketNumber)
}

func (h *sentPacketHandler) SentPacket(packet *ackhandlerlegacy.Packet) error {
	if packet.PacketNumber <= h.lastSentPacketNumber {
		return errPacketNumberNotIncreasing
	}

	for p := h.lastSentPacketNumber + 1; p < packet.PacketNumber; p++ {
		h.skippedPackets = append(h.skippedPackets, p)

		if len(h.skippedPackets) > protocol.MaxTrackedSkippedPackets {
			h.skippedPackets = h.skippedPackets[1:]
		}
	}

	now := time.Now()
	h.lastSentPacketTime = now
	packet.SendTime = now
	if packet.Length == 0 {
		return errors.New("SentPacketHandler: packet cannot be empty")
	}
	h.bytesInFlight += packet.Length

	h.lastSentPacketNumber = packet.PacketNumber
	h.packetHistory.PushBack(*packet)

	h.congestion.OnPacketSent(
		time.Now(),
		h.BytesInFlight(),
		packet.PacketNumber,
		packet.Length,
		true, /* TODO: is retransmittable */
	)

	return nil
}

func (h *sentPacketHandler) ReceivedAck(ackFrame *frames.AckFrame, withPacketNumber protocol.PacketNumber) error {
	if ackFrame.LargestAcked > h.lastSentPacketNumber {
		return errAckForUnsentPacket
	}

	// duplicate or out-of-order ACK
	if withPacketNumber <= h.largestReceivedPacketWithAck {
		return ErrDuplicateOrOutOfOrderAck
	}

	h.largestReceivedPacketWithAck = withPacketNumber

	// ignore repeated ACK (ACKs that don't have a higher LargestAcked than the last ACK)
	if ackFrame.LargestAcked <= h.LargestInOrderAcked {
		return nil
	}

	// check if it acks any packets that were skipped
	for _, p := range h.skippedPackets {
		if ackFrame.AcksPacket(p) {
			return ErrAckForSkippedPacket
		}
	}

	h.LargestAcked = ackFrame.LargestAcked

	var ackedPackets congestion.PacketVector
	var lostPackets congestion.PacketVector
	ackRangeIndex := 0

	var el, elNext *ackhandlerlegacy.PacketElement
	for el = h.packetHistory.Front(); el != nil; el = elNext {
		// determine the next list element right at the beginning, because el.Next() is not avaible anymore, when the list element is deleted (i.e. when the packet is ACKed)
		elNext = el.Next()
		packet := el.Value
		packetNumber := packet.PacketNumber

		// NACK packets below the LowestAcked
		if packetNumber < ackFrame.LowestAcked {
			p, err := h.nackPacket(el)
			if err != nil {
				return err
			}
			if p != nil {
				lostPackets = append(lostPackets, congestion.PacketInfo{Number: p.PacketNumber, Length: p.Length})
			}
			continue
		}

		// Update the RTT
		if packetNumber == h.LargestAcked {
			timeDelta := time.Now().Sub(packet.SendTime)
			// TODO: Don't always update RTT
			h.rttStats.UpdateRTT(timeDelta, ackFrame.DelayTime, time.Now())
			if utils.Debug() {
				utils.Debugf("\tEstimated RTT: %dms", h.rttStats.SmoothedRTT()/time.Millisecond)
			}
		}

		if packetNumber > ackFrame.LargestAcked {
			break
		}

		if ackFrame.HasMissingRanges() {
			ackRange := ackFrame.AckRanges[len(ackFrame.AckRanges)-1-ackRangeIndex]

			if packetNumber > ackRange.LastPacketNumber && ackRangeIndex < len(ackFrame.AckRanges)-1 {
				ackRangeIndex++
				ackRange = ackFrame.AckRanges[len(ackFrame.AckRanges)-1-ackRangeIndex]
			}

			if packetNumber >= ackRange.FirstPacketNumber { // packet i contained in ACK range
				p := h.ackPacket(el)
				if p != nil {
					ackedPackets = append(ackedPackets, congestion.PacketInfo{Number: p.PacketNumber, Length: p.Length})
				}
			} else {
				p, err := h.nackPacket(el)
				if err != nil {
					return err
				}
				if p != nil {
					lostPackets = append(lostPackets, congestion.PacketInfo{Number: p.PacketNumber, Length: p.Length})
				}
			}
		} else {
			p := h.ackPacket(el)
			if p != nil {
				ackedPackets = append(ackedPackets, congestion.PacketInfo{Number: p.PacketNumber, Length: p.Length})
			}
		}
	}

	h.garbageCollectSkippedPackets()

	h.stopWaitingManager.ReceivedAck(ackFrame)

	h.congestion.OnCongestionEvent(
		true, /* TODO: rtt updated */
		h.BytesInFlight(),
		ackedPackets,
		lostPackets,
	)

	return nil
}

// ProbablyHasPacketForRetransmission returns if there is a packet queued for retransmission
// There is one case where it gets the answer wrong:
// if a packet has already been queued for retransmission, but a belated ACK is received for this packet, this function will return true, although the packet will not be returend for retransmission by DequeuePacketForRetransmission()
func (h *sentPacketHandler) ProbablyHasPacketForRetransmission() bool {
	h.maybeQueuePacketsRTO()
	return len(h.retransmissionQueue) > 0
}

func (h *sentPacketHandler) DequeuePacketForRetransmission() *ackhandlerlegacy.Packet {
	if !h.ProbablyHasPacketForRetransmission() {
		return nil
	}

	if len(h.retransmissionQueue) > 0 {
		queueLen := len(h.retransmissionQueue)
		// packets are usually NACKed in descending order. So use the slice as a stack
		packet := h.retransmissionQueue[queueLen-1]
		h.retransmissionQueue = h.retransmissionQueue[:queueLen-1]
		return packet
	}

	return nil
}

func (h *sentPacketHandler) BytesInFlight() protocol.ByteCount {
	return h.bytesInFlight
}

func (h *sentPacketHandler) GetLeastUnacked() protocol.PacketNumber {
	return h.LargestInOrderAcked + 1
}

func (h *sentPacketHandler) GetStopWaitingFrame() *frames.StopWaitingFrame {
	return h.stopWaitingManager.GetStopWaitingFrame()
}

func (h *sentPacketHandler) CongestionAllowsSending() bool {
	return h.BytesInFlight() <= h.congestion.GetCongestionWindow()
}

func (h *sentPacketHandler) CheckForError() error {
	length := len(h.retransmissionQueue) + h.packetHistory.Len()
	if uint32(length) > protocol.MaxTrackedSentPackets {
		return ErrTooManyTrackedSentPackets
	}
	return nil
}

func (h *sentPacketHandler) maybeQueuePacketsRTO() {
	if time.Now().Before(h.TimeOfFirstRTO()) {
		return
	}

	for el := h.packetHistory.Front(); el != nil; el = el.Next() {
		packet := &el.Value
		if packet.PacketNumber < h.LargestInOrderAcked {
			continue
		}

		packetsLost := congestion.PacketVector{congestion.PacketInfo{
			Number: packet.PacketNumber,
			Length: packet.Length,
		}}
		h.congestion.OnCongestionEvent(false, h.BytesInFlight(), nil, packetsLost)
		h.congestion.OnRetransmissionTimeout(true)
		// utils.Debugf("\tqueueing RTO retransmission for packet 0x%x", packet.PacketNumber)
		h.queuePacketForRetransmission(el)
		return
	}
}

func (h *sentPacketHandler) getRTO() time.Duration {
	rto := h.congestion.RetransmissionDelay()
	if rto == 0 {
		rto = protocol.DefaultRetransmissionTime
	}
	return utils.MaxDuration(rto, protocol.MinRetransmissionTime)
}

func (h *sentPacketHandler) TimeOfFirstRTO() time.Time {
	if h.lastSentPacketTime.IsZero() {
		return time.Time{}
	}
	return h.lastSentPacketTime.Add(h.getRTO())
}

func (h *sentPacketHandler) garbageCollectSkippedPackets() {
	deleteIndex := 0
	for i, p := range h.skippedPackets {
		if p <= h.LargestInOrderAcked {
			deleteIndex = i + 1
		}
	}
	h.skippedPackets = h.skippedPackets[deleteIndex:]
}
