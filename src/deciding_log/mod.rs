use std::collections::VecDeque;
use either::Either;
use log::{error, warn};
use atlas_common::ordering::{InvalidSeqNo, Orderable, SeqNo};
use atlas_common::serialization_helper::SerType;
use atlas_core::ordering_protocol::{DecisionMetadata, ProtocolConsensusDecision};
use atlas_core::ordering_protocol::networking::serialize::OrderingProtocolMessage;
use atlas_core::smr::smr_decision_log::ShareableConsensusMessage;
use atlas_smr_application::app::UpdateBatch;
use atlas_smr_application::serialize::ApplicationData;
use crate::decisions::{CompletedDecision, OnGoingDecision};


/// The log for decisions which are currently being decided
pub struct DecidingLog<RQ, OP, PL>
    where RQ: SerType, OP: OrderingProtocolMessage<RQ> {
    // The seq no of the first decision in the queue
    // Therefore it is the sequence number of the first decision we are working on
    curr_seq: SeqNo,

    // The currently deciding list. This is a vec deque since we can only decide seqno n when
    // all seqno < n have already been decided
    currently_deciding: VecDeque<OnGoingDecision<RQ, OP>>,

    // A reference to the persistent log so we can immediately begin the storage process
    persistent_log: PL,
}

impl<RQ, OP, PL> DecidingLog<RQ, OP, PL>
    where RQ: SerType, OP: OrderingProtocolMessage<RQ> {
    pub fn init(default_capacity: usize, starting_seq: SeqNo, persistent_log: PL) -> Self {
        Self {
            curr_seq: starting_seq,
            currently_deciding: VecDeque::with_capacity(default_capacity),
            persistent_log,
        }
    }

    pub fn clear_decision_at(&mut self, seq: SeqNo) {
        let index = seq.index(self.curr_seq);

        match index {
            Either::Left(_) => {
                warn!("Attempted to clear decision which has already been decided")
            }
            Either::Right(index) => {
                self.currently_deciding[index] = OnGoingDecision::init(seq);
            }
        }
    }

    pub fn clear_seq_forward_of(&mut self, seq: SeqNo) {
        let index = seq.index(self.curr_seq);

        match index {
            Either::Right(index) => {
                let to_remove = self.currently_deciding.len() - index;

                for _ in 0..to_remove {
                    self.currently_deciding.pop_back();
                }
            }
            Either::Left(_) => {
                warn!("Progressed decision that has already been decided?")
            }
        }
    }

    /// Advance to the given sequence number, ignoring all of the decisions until then
    pub fn advance_to_seq(&mut self, seq: SeqNo) {
        match seq.index(self.curr_seq) {
            Either::Left(_) | Either::Right(0) => {
                warn!("How can we advance to a sequence number we are already at?");

                return;
            }
            Either::Right(index) => {
                for _ in 0..index {
                    self.currently_deciding.pop_front();
                }
            }
        }

        self.curr_seq = seq;
    }

    pub fn reset_to_zero(&mut self) {
        self.curr_seq = SeqNo::ZERO;
        self.currently_deciding.clear();
    }

    fn decision_at_index(&mut self, index: usize) -> &mut OnGoingDecision<RQ, OP> {
        if self.currently_deciding.len() > index {
            self.currently_deciding.get_mut(index).unwrap()
        } else {
            let to_create = (index - self.currently_deciding.len()) + 1;

            let mut start_seq = self.currently_deciding.back()
                .map(|decision| decision.sequence_number().next())
                .unwrap_or(self.curr_seq);

            for _ in 0..=to_create {
                self.currently_deciding.push_back(OnGoingDecision::init(start_seq));

                start_seq = start_seq.next();
            }

            self.currently_deciding.get_mut(index).unwrap()
        }
    }

    pub fn decision_progressed(&mut self, seq: SeqNo, message: ShareableConsensusMessage<RQ, OP>) {
        let index = seq.index(self.curr_seq);

        match index {
            Either::Right(index) => {
                self.decision_at_index(index).insert_component_message(message);
            }
            Either::Left(_) => {
                warn!("Progressed decision that has already been decided?")
            }
        }
    }

    pub fn decision_metadata(&mut self, seq: SeqNo, metadata: DecisionMetadata<RQ, OP>) {
        let index = seq.index(self.curr_seq);

        match index {
            Either::Right(index) => {
                let decision = self.decision_at_index(index);

                decision.insert_metadata(metadata);
            }
            Either::Left(_) => {
                warn!("Completing decision that has already been decided")
            }
        }
    }

    pub fn complete_decision(&mut self, seq: SeqNo, decision_info: ProtocolConsensusDecision<RQ>) {
        let index = seq.index(self.curr_seq);

        match index {
            Either::Right(index) => {
                let decision = self.decision_at_index(index);

                decision.insert_requests(decision_info);
                decision.completed();
            }
            Either::Left(_) => {
                warn!("Completing decision that has already been decided")
            }
        }
    }

    /// Get the pending decisions that already have all of the necessary information
    /// to be completed
    pub fn complete_pending_decisions(&mut self) -> Vec<CompletedDecision<RQ, OP>> {
        let mut decisions = vec![];

        while !self.currently_deciding.is_empty() {
            if self.currently_deciding.front().unwrap().is_completed() {
                let decision = self.currently_deciding.pop_front().unwrap();

                decisions.push(decision.into_completed_decision());

                self.curr_seq = self.curr_seq.next();
            } else {
                break;
            }
        }

        decisions
    }
}
