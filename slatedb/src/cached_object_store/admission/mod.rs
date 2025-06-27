use std::fmt::Debug;
use std::sync::Arc;

use crate::cached_object_store::LocalCacheEntry;

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

    #[allow(unused)]
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

pub trait AdmissionPolicy: Send + Sync + 'static + Debug {
    /// Determines whether the given entry should be admitted to the disk cache.
    fn pick(&self, entry: &dyn LocalCacheEntry) -> AdmitResult;

    /// A human-readable name for the policy
    #[allow(unused)]
    fn name(&self) -> &str;
}

/// A policy that admits all objects to the disk cache without any checks.
#[derive(Debug, Clone)]
pub struct AdmitAll;

impl AdmissionPolicy for AdmitAll {
    fn pick(&self, _: &dyn LocalCacheEntry) -> AdmitResult {
        AdmitResult::Admitted
    }

    fn name(&self) -> &str {
        "AdmitAll"
    }
}

/// The admission picker is responsible for deciding whether an object should be admitted to the disk cache or not.
#[derive(Debug, Clone)]
pub struct AdmissionPicker {
    policies: Vec<Arc<dyn AdmissionPolicy>>,
}

impl AdmissionPicker {
    pub fn new() -> Self {
        Self {
            policies: vec![Arc::new(AdmitAll)],
        }
    }

    #[allow(unused)]
    pub fn build_with_policies(policies: Vec<Arc<dyn AdmissionPolicy>>) -> Self {
        Self { policies }
    }

    pub fn pick(&self, entry: &dyn LocalCacheEntry) -> AdmitResult {
        for policy in self.policies.iter() {
            match policy.pick(entry) {
                AdmitResult::Admitted => {}
                AdmitResult::Rejected => return AdmitResult::Rejected,
            }
        }

        AdmitResult::Admitted
    }
}

impl Default for AdmissionPicker {
    fn default() -> Self {
        Self::new()
    }
}
