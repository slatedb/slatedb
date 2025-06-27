use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub enum AdmitResult {
    /// The object was admitted to the disk cache.
    Admitted,
    /// The object was rejected
    Rejected,
}

impl AdmitResult {
    pub fn admitted(&self) -> bool {
        matches!(self, AdmitResult::Admitted)
    }

    pub fn rejected(&self) -> bool {
        matches!(self, AdmitResult::Rejected)
    }
}

impl From<bool> for AdmitResult {
    fn from(value: bool) -> Self {
        if value {
            AdmitResult::Admitted
        } else {
            AdmitResult::Rejected
        }
    }
}

pub trait AdmissionPolicy: Send + Sync + 'static + Debug {}

/// The admission picker is responsible for deciding whether an object should be admitted to the disk cache or not.
#[derive(Debug, Clone)]
pub struct AdmissionPicker {
    pub policies: Vec<Arc<dyn AdmissionPolicy>>,
}
