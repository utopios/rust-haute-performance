use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use rand::distr::{Distribution, Uniform};

fn main() {
    let mut rng_alea = rand::rng();
    let x: u64 = rng_alea.random();
    let y: u32 = rng_alea.random_range(1..=100);
    println!("Aleatoire pur     : u64={}, dans [1..=100]={}", x, y);

    let mut rng_seed = StdRng::seed_from_u64(42);
    let a: u64 = rng_seed.random();
    let b: u32 = rng_seed.random_range(1..=100);
    println!("Avec seed=42      : u64={}, dans [1..=100]={}", a, b);

    let mut rng = StdRng::seed_from_u64(42);
    let dist = Uniform::new_inclusive(1u64, 5000).unwrap();
    let echantillon: Vec<u64> = (0..10).map(|_| dist.sample(&mut rng)).collect();
    println!("10 latences (1..=5000): {:?}", echantillon);

    let mut rng = StdRng::seed_from_u64(42);
    let dist = Uniform::new_inclusive(1u64, 5000).unwrap();
    let data: Vec<u64> = (0..100_000).map(|_| dist.sample(&mut rng)).collect();
    let somme: u64 = data.iter().sum();
    println!("Dataset 100k      : somme={}, moyenne={:.1}", somme, somme as f64 / data.len() as f64);
}
