use gossip_sim::{ConvergenceComparison, ConvergenceExperiment, ConvergenceReport, NetworkModel};

fn percent(rate: f64) -> f64 {
    rate * 100.0
}

fn main() {
    let reliable = ConvergenceExperiment::new(25, 3, 12, 100)
        .expect("experiment parameters should be valid")
        .with_seed(42);

    let lossy_network = NetworkModel::new()
        .with_loss_rate(0.10)
        .expect("valid loss rate");

    let lossy = ConvergenceExperiment::new(25, 3, 20, 100)
        .expect("experiment parameters should be valid")
        .with_seed(42)
        .with_network_model(lossy_network);

    let delayed_network = NetworkModel::new()
        .with_delay_rate(0.25, 3)
        .expect("valid delay rate");

    let delayed = ConvergenceExperiment::new(25, 3, 20, 100)
        .expect("experiment parameters should be valid")
        .with_seed(42)
        .with_network_model(delayed_network);

    let comparison = ConvergenceComparison::new()
        .add("Reliable network", reliable)
        .add("Lossy network", lossy)
        .add("Delayed network", delayed)
        .run();

    for (index, result) in comparison.results().iter().enumerate() {
        if index > 0 {
            println!();
        }

        print_report(result.label(), result.report());
    }
}

fn print_report(label: &str, report: &ConvergenceReport) {
    println!("{label}");
    println!("  trials: {}", report.trials());
    println!("  successes: {}", report.successes());
    println!("  failures: {}", report.failures());
    println!("  success rate: {:.2}%", percent(report.success_rate()));
    println!("  failure rate: {:.2}%", percent(report.failure_rate()));
    println!(
        "  attempted: {} ({:.2}/trial)",
        report.attempted(),
        report.mean_attempted_per_trial()
    );
    println!(
        "  sent: {} ({:.2}/trial)",
        report.sent(),
        report.mean_sent_per_trial()
    );
    println!(
        "  dropped: {} ({:.2}/trial)",
        report.dropped(),
        report.mean_dropped_per_trial()
    );
    println!(
        "  duplicated: {} ({:.2}/trial)",
        report.duplicated(),
        report.mean_duplicated_per_trial()
    );
    println!(
        "  delayed: {} ({:.2}/trial)",
        report.delayed(),
        report.mean_delayed_per_trial()
    );
    println!(
        "  received: {} ({:.2}/trial)",
        report.received(),
        report.mean_received_per_trial()
    );
    println!(
        "  observed drop rate: {:.2}%",
        percent(report.observed_drop_rate())
    );
    println!(
        "  observed duplicate rate: {:.2}%",
        percent(report.observed_duplicate_rate())
    );
    println!(
        "  observed delay rate: {:.2}%",
        percent(report.observed_delay_rate())
    );
    println!(
        "  observed delivery rate: {:.2}%",
        percent(report.observed_delivery_rate())
    );

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
