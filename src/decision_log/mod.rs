use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::error::*;
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage, OrderProtocolProof};
use atlas_core::ordering_protocol::loggable::{LoggableOrderProtocol, PersistentOrderProtocolTypes, PProof};
use atlas_smr_application::serialize::ApplicationData;

#[derive(Clone)]
pub struct DecisionLog<D, OP, POP> where D: ApplicationData,
                                         OP: OrderingProtocolMessage<D>,
                                         POP: PersistentOrderProtocolTypes<D, OP> {
    last_exec: Option<SeqNo>,
    decided: Vec<PProof<D, OP, POP>>,
}

impl<D, OP, POP> DecisionLog<D, OP, POP> where D: ApplicationData,
                                               OP: OrderingProtocolMessage<D>,
                                               POP: PersistentOrderProtocolTypes<D, OP> {
    pub fn new() -> Self {
        Self {
            last_exec: None,
            decided: vec![],
        }
    }

    /// Initialize a decision log from a given vector of proofs
    pub fn from_decided(last_exec: SeqNo, proofs: Vec<PProof<D, OP, POP>>) -> Self {
        Self {
            last_exec: Some(last_exec),
            decided: proofs,
        }
    }

    /// Assemble a decision log from a vector of proofs
    pub fn from_proofs(mut proofs: Vec<PProof<D, OP, POP>>) -> Self {
        proofs.sort_by(|a, b| a.sequence_number().cmp(&b.sequence_number()).reverse());

        let last_decided = proofs.last().map(|proof| proof.sequence_number());

        Self {
            last_exec: last_decided,
            decided: proofs,
        }
    }

    pub fn from_ordered_proofs(proofs: Vec<PProof<D, OP, POP>>) -> Self {
        let last_decided = proofs.last().map(|proof| proof.sequence_number());

        Self {
            last_exec: last_decided,
            decided: proofs,
        }
    }

    /// Returns the sequence number of the last executed batch of client
    /// requests, assigned by the conesensus layer.
    pub fn last_execution(&self) -> Option<SeqNo> {
        self.last_exec
    }

    /// Get all of the decided proofs in this decisionn log
    pub fn proofs(&self) -> &[PProof<D, OP, POP>] {
        &self.decided[..]
    }

    /// Append a proof to the end of the log. Assumes all prior checks have been done
    pub(crate) fn append_proof(&mut self, proof: PProof<D, OP, POP>) {
        self.last_exec = Some(proof.seq_no());

        self.decided.push(proof);
    }

    //TODO: Maybe make these data structures a BTreeSet so that the messages are always ordered
    //By their seq no? That way we cannot go wrong in the ordering of messages.
    pub(crate) fn finished_quorum_execution(&mut self, proof: &PProof<D, OP, POP>, seq_no: SeqNo) -> Result<()> {
        self.last_exec.replace(seq_no);

        self.decided.push(proof);

        Ok(())
    }

    /// Returns the proof of the last executed consensus
    /// instance registered in this `DecisionLog`.
    pub fn last_decision(&self) -> Option<PProof<D, OP, POP>> {
        self.decided.last().map(|p| (*p).clone())
    }

    /// Clear the decision log until the given sequence number
    pub(crate) fn clear_until_seq(&mut self, seq_no: SeqNo) -> usize {
        let mut net_decided = Vec::with_capacity(self.decided.len());

        let mut decided_request_count = 0;

        let prev_decided = std::mem::replace(&mut self.decided, net_decided);

        for proof in prev_decided.into_iter().rev() {
            if proof.sequence_number() <= seq_no {
                decided_request_count += proof.contained_messages();
            } else {
                self.decided.push(proof);
            }
        }

        self.decided.reverse();

        decided_request_count
    }

    pub(crate) fn into_proofs(self) -> Vec<PProof<D, OP, POP>> {
        self.decided
    }
}