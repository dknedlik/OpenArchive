use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

fn cosine_similarity(left: &[f32], right: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut left_norm = 0.0f32;
    let mut right_norm = 0.0f32;
    for (l, r) in left.iter().zip(right.iter()) {
        dot += l * r;
        left_norm += l * l;
        right_norm += r * r;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        return 0.0;
    }
    dot / (left_norm.sqrt() * right_norm.sqrt())
}

struct UnionFind {
    parents: Vec<usize>,
}

impl UnionFind {
    fn new(size: usize) -> Self {
        Self {
            parents: (0..size).collect(),
        }
    }

    fn find(&mut self, index: usize) -> usize {
        if self.parents[index] != index {
            let root = self.find(self.parents[index]);
            self.parents[index] = root;
        }
        self.parents[index]
    }

    fn union(&mut self, left: usize, right: usize) {
        let left_root = self.find(left);
        let right_root = self.find(right);
        if left_root != right_root {
            self.parents[right_root] = left_root;
        }
    }
}

fn main() {
    let texts = vec![
        "Body Weight",
        "Current body weight",
        "LDL Cholesterol Jump",
        "LDL Jump from Beef/Pork",
        "LDL rise from 109 to 191 on high saturated fat",
        "Daily Sardine Consumption",
        "Daily Sardine Habit",
        "Daily Sardines and Smoked Salmon Intake",
        "Keto Diet and Lifestyle",
        "Keto-Adjacent Diet Preference",
        "Protein rotation: more fish, less beef/pork",
        "Updated Fish Rotation Protocol",
        "Breakfast Fish Rotation",
        "Tinned fish breakfast rotation for cost savings",
        "Preferred Nuts for Snacking",
        "Preferred nuts: macadamia, then pecans or hazelnuts",
        "Salting Drinking Water for Keto",
        "Salts water regularly",
    ];

    let mut model = TextEmbedding::try_new(InitOptions::new(EmbeddingModel::BGESmallENV15))
        .expect("embedding model should load");
    let refs: Vec<&str> = texts.iter().copied().collect();
    let vectors = model.embed(refs, None).expect("embeddings should generate");

    let mut uf = UnionFind::new(texts.len());
    for i in 0..texts.len() {
        for j in (i + 1)..texts.len() {
            let similarity = cosine_similarity(&vectors[i], &vectors[j]);
            if similarity >= 0.80 {
                uf.union(i, j);
            }
        }
    }

    let mut groups = std::collections::BTreeMap::<usize, Vec<usize>>::new();
    for index in 0..texts.len() {
        groups.entry(uf.find(index)).or_default().push(index);
    }

    println!("threshold=0.80");
    for indices in groups.values() {
        if indices.len() > 1 {
            println!("cluster:");
            for &index in indices {
                println!("  - {}", texts[index]);
            }
        }
    }

    println!("\nselected pair sims:");
    for (left, right) in [
        (0, 1),
        (2, 3),
        (2, 4),
        (5, 6),
        (5, 7),
        (8, 9),
        (10, 11),
        (12, 13),
        (14, 15),
        (16, 17),
    ] {
        println!(
            "{:.4}\t{} || {}",
            cosine_similarity(&vectors[left], &vectors[right]),
            texts[left],
            texts[right]
        );
    }
}
