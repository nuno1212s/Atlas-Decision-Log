use atlas_common::ordering::{Orderable, SeqNo};
use atlas_core::ordering_protocol::{DecisionMetadata, ProtocolConsensusDecision};
use atlas_core::ordering_protocol::networking::serialize::OrderingProtocolMessage;
use atlas_core::smr::smr_decision_log::{LoggingDecision, ShareableConsensusMessage};
use atlas_smr_application::serialize::ApplicationData;

/// A struct to store the ongoing decision known parameters
pub struct OnGoingDecision<D, OP>
    where D: ApplicationData,
          OP: OrderingProtocolMessage<D> {
    // The seq number of this decision
    seq: SeqNo,
    // Whether this decision has been marked as completed by the ordering protocol
    completed: bool,
    // The metadata of the decision, optional since it's usually the
    metadata: Option<DecisionMetadata<D, OP>>,
    // The messages that compose this decision, to be transformed into a given proof
    messages: Vec<ShareableConsensusMessage<D, OP>>,
    // The decision information from the ordering protocol
    protocol_decision: Option<ProtocolConsensusDecision<D::Request>>,
    // The information about the decision that is being logged.
    // This is what is going to be used to send to the persistent
    // Logging layer in order to better control when a given sequence
    // number is completely persisted
    logging_decision: LoggingDecision,
}

/// The completed decision object with all necessary information to be transformed
/// into a proof, which will be put into the decision log
pub struct CompletedDecision<D, OP>
    where D: ApplicationData,
          OP: OrderingProtocolMessage<D> {
    seq: SeqNo,
    metadata: DecisionMetadata<D, OP>,
    messages: Vec<ShareableConsensusMessage<D, OP>>,
    protocol_decision: ProtocolConsensusDecision<D::Request>,
    logged_info: LoggingDecision,
}

impl<D, OP> Orderable for OnGoingDecision<D, OP>
    where D: ApplicationData, OP: OrderingProtocolMessage<D> {
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl<D, OP> OnGoingDecision<D, OP>
    where D: ApplicationData,
          OP: OrderingProtocolMessage<D> {
    pub fn init(seq: SeqNo) -> Self {
        Self {
            seq,
            completed: false,
            metadata: None,
            messages: vec![],
            protocol_decision: None,
            logging_decision: LoggingDecision::init_empty(seq),
        }
    }

    pub fn insert_metadata(&mut self, metadata: DecisionMetadata<D, OP>) {
        let _ = self.metadata.insert(metadata);
    }

    pub fn insert_component_message(&mut self, partial: ShareableConsensusMessage<D, OP>) {
        self.logging_decision.insert_message::<D, OP>(&partial);

        self.messages.push(partial);
    }

    pub fn insert_requests(&mut self, protocol_decision: ProtocolConsensusDecision<D::Request>) {
        self.protocol_decision = Some(protocol_decision)
    }

    pub fn completed(&mut self) {
        self.completed = true;
    }

    pub fn is_completed(&self) -> bool {
        self.completed
    }

    pub fn into_completed_decision(self) -> CompletedDecision<D, OP> {
        CompletedDecision {
            seq: self.seq,
            metadata: self.metadata.unwrap(),
            messages: self.messages,
            protocol_decision: self.protocol_decision.unwrap(),
            logged_info: self.logging_decision,
        }
    }
}

impl<D, OP> CompletedDecision<D, OP>
    where D: ApplicationData,
          OP: OrderingProtocolMessage<D> {
    pub fn into(self) -> (SeqNo, DecisionMetadata<D, OP>,
                          Vec<ShareableConsensusMessage<D, OP>>,
                          ProtocolConsensusDecision<D::Request>,
                          LoggingDecision) {
        (self.seq, self.metadata, self.messages, self.protocol_decision, self.logged_info)
    }
}

