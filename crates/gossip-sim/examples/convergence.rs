use gossip_sim::ConvergenceExperiment;

fn main() {
    let experiment = ConvergenceExperiment::new(25, 3, 12, 100)
        .expect("experiment parameters should be valid")
        .with_seed(42);

    let report = experiment.run();

    println!("Convergence experiment");
    println!("  trials: {}", report.trials());
    println!("  successes: {}", report.successes());
    println!("  success rate: {:.2}%", report.success_rate() * 100.0);

    match report.mean_successful_rounds() {
        Some(mean) => println!("  mean successful rounds: {mean:.2}"),
        None => println!("  mean successful rounds: n/a"),
    }

    match report.percentile_successful_rounds(50.0) {
        Some(p50) => println!("  p50 successful rounds: {p50}"),
        None => println!("  p50 successful rounds: n/a"),
    }

    match report.percentile_successful_rounds(95.0) {
        Some(p95) => println!("  p95 successful rounds: {p95}"),
        None => println!("  p95 successful rounds: n/a"),
    }
}
