//! Validator registry mapping string IDs to shared descriptor trees.

use std::collections::HashMap;
use std::sync::Arc;

use xml_oxydizer::rule::Rule;
use xml_oxydizer::tree::descriptor::DescriptorTree;

/// Maps validator ID strings to `Arc<DescriptorTree>` instances.
///
/// Built once at startup via [`ValidatorRegistry::register`], then shared
/// read-only across gRPC request handlers via `Arc<ValidatorRegistry>`.
pub struct ValidatorRegistry {
    trees: HashMap<String, Arc<DescriptorTree<Box<dyn Rule>>>>,
}

impl Default for ValidatorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidatorRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self {
            trees: HashMap::new(),
        }
    }

    /// Registers a descriptor tree under the given validator ID.
    pub fn register(&mut self, id: &str, tree: DescriptorTree<Box<dyn Rule>>) {
        self.trees.insert(id.to_owned(), Arc::new(tree));
    }

    /// Looks up a descriptor tree by validator ID.
    pub fn get(&self, id: &str) -> Option<Arc<DescriptorTree<Box<dyn Rule>>>> {
        self.trees.get(id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xml_oxydizer::tree::builder::TreeBuilder;

    #[test]
    fn register_and_get() {
        let mut reg = ValidatorRegistry::new();

        let tree = TreeBuilder::<Box<dyn Rule>>::new("root")
            .streaming()
            .build()
            .unwrap();

        reg.register("test-validator", tree);

        assert!(reg.get("test-validator").is_some());
        assert!(reg.get("nonexistent").is_none());
    }

    #[test]
    fn get_returns_shared_arc() {
        let mut reg = ValidatorRegistry::new();

        let tree = TreeBuilder::<Box<dyn Rule>>::new("root")
            .streaming()
            .build()
            .unwrap();
        reg.register("v1", tree);

        let a = reg.get("v1").unwrap();
        let b = reg.get("v1").unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }
}
