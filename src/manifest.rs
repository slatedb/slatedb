use crate::db_state::CoreDbState;
use crate::error::SlateDBError;
use bytes::Bytes;
use std::fmt;
use std::fmt::Formatter;

#[derive(Clone)]
pub(crate) struct Manifest {
    pub(crate) core: CoreDbState,
    pub(crate) writer_epoch: u64,
    pub(crate) compactor_epoch: u64,
}

impl fmt::Display for Manifest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let core_indented = self
            .core
            .to_string()
            .lines()
            .map(|line| format!("\t{}", line))
            .collect::<Vec<String>>()
            .join("\n");

        write!(
            f,
            "{{\n\tCore: {},\n\tWriter Epoch: {},\n\tCompactor Epoch: {}\n}}",
            core_indented.trim(),
            self.writer_epoch,
            self.compactor_epoch
        )
    }
}

pub(crate) trait ManifestCodec: Send + Sync {
    fn encode(&self, manifest: &Manifest) -> Bytes;

    fn decode(&self, bytes: &Bytes) -> Result<Manifest, SlateDBError>;
}
