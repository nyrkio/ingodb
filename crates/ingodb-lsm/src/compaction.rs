use ingodb_blob::{DocumentId, IBlob};
use std::path::PathBuf;

/// Action to take on a blob during compaction.
pub enum CompactionAction {
    /// Keep the blob as-is
    Keep,
    /// Drop the blob (e.g., tombstone purge)
    Drop,
    /// Replace with a transformed blob (e.g., semantic shredding)
    Transform(IBlob),
}

/// Pluggable filter applied during compaction.
/// This is the extension point for future adaptive morphing.
pub trait CompactionFilter {
    fn filter(&mut self, id: &DocumentId, blob: &IBlob) -> CompactionAction;
}

/// Size-Tiered Compaction Strategy.
///
/// Groups SSTables by similar file size and triggers compaction
/// when a group reaches the threshold. This is the baseline strategy;
/// UCS (Universal Compaction Strategy) will build on top of this.
pub struct SizeTieredCompaction {
    threshold: usize,
}

impl SizeTieredCompaction {
    pub fn new(threshold: usize) -> Self {
        SizeTieredCompaction { threshold }
    }

    /// Pick SSTables to compact, if any group has reached the threshold.
    /// Returns the paths of SSTables to merge, or None.
    pub fn pick_compaction(&self, sst_paths: &[PathBuf]) -> Option<Vec<PathBuf>> {
        if sst_paths.len() < self.threshold {
            return None;
        }

        // Get file sizes
        let mut sized: Vec<(PathBuf, u64)> = sst_paths
            .iter()
            .filter_map(|p| {
                std::fs::metadata(p)
                    .ok()
                    .map(|m| (p.clone(), m.len()))
            })
            .collect();

        sized.sort_by_key(|(_, size)| *size);

        // Group by similar size (within 2x of each other)
        let mut groups: Vec<Vec<PathBuf>> = Vec::new();
        let mut current_group: Vec<(PathBuf, u64)> = Vec::new();

        for (path, size) in sized {
            if current_group.is_empty() {
                current_group.push((path, size));
            } else {
                let group_min = current_group[0].1;
                if size <= group_min * 2 {
                    current_group.push((path, size));
                } else {
                    if current_group.len() >= self.threshold {
                        groups.push(current_group.iter().map(|(p, _)| p.clone()).collect());
                    }
                    current_group = vec![(path, size)];
                }
            }
        }
        if current_group.len() >= self.threshold {
            groups.push(current_group.iter().map(|(p, _)| p.clone()).collect());
        }

        // Return the first group that reached threshold
        groups.into_iter().next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_compaction_needed() {
        let compaction = SizeTieredCompaction::new(4);
        let paths = vec![PathBuf::from("a.sst"), PathBuf::from("b.sst")];
        assert!(compaction.pick_compaction(&paths).is_none());
    }
}
