mod assertions;
mod aws_ci;
mod aws_rollout;
mod aws_simple;
mod chart;
mod compute_lifecycle;
mod database_roles;
mod database_test;
mod engine_compare;
mod engine_lane;
mod infra_variants;
mod kind_checks;
mod kind_transform;
mod long_deadline;
mod network;
mod object_store;
mod operator;
mod scale;
mod team;
mod util;
mod worker_resources;

use anyhow::{Context, Result, bail};

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("a rustls crypto provider was already installed"))?;
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        bail!("an operations subcommand is required");
    }
    let command = args.remove(0);
    match command.as_str() {
        "assert-drain-order" => assertions::assert_drain_order(args),
        "assert-draining" => assertions::assert_draining(args),
        "database-roles" => database_roles::run(args),
        "check-chart" => chart::check_chart(args),
        "compute-lifecycle" => compute_lifecycle::run(args),
        "compare-engine-qualification" => engine_compare::run(args),
        "qualify-engine" => engine_lane::qualify(args),
        "helm-values" => infra_variants::helm_values(args),
        "deploy-overlay" => infra_variants::deploy_overlay(args),
        "chart-scaled" => infra_variants::chart_scaled(args),
        "chart-autoscaling" => infra_variants::chart_autoscaling(args),
        "chart-flip" => infra_variants::chart_flip(args),
        "boundary-disjoint" => infra_variants::boundary_disjoint(args),
        "scale-generate" => scale::run(args),
        "test-database-roles" => database_test::run(args),
        "object-store" => object_store::run(args).await,
        "test-long-deadline" => long_deadline::run(args).await,
        "migration-clone" => operator::migration_clone(args),
        "migration-deadline" => operator::migration_deadline(args),
        "api-egress-config" => operator::api_egress_config(args),
        "external-postgres" => operator::external_postgres(args),
        "boundary-summary" => operator::boundary_summary(args),
        "check-role-grant" => operator::check_role_grant(args),
        "report-nodepool" => operator::report_nodepool(args),
        "chart-secret-name" => operator::chart_secret_name(args),
        "random-id" => operator::random_id(args),
        "free-ports" => operator::free_ports(args),
        "await-health" => operator::await_health(args).await,
        "kind-assert-limits" => kind_checks::assert_limits(args),
        "kind-assert-role-posture" => kind_checks::assert_role_posture(args),
        "kind-check-networkpolicy" => kind_checks::check_networkpolicy(args),
        "kind-localize" => kind_transform::localize(args),
        "kind-harness-job" => kind_transform::harness_job(args),
        "kind-await-job" => kind_transform::await_job(args),
        "kind-team-namespace-probe" => team::run_probe(args).await,
        "kind-gateway-ownership" => team::run_gateway_ownership(args).await,
        "aws-ci-test" => aws_ci::run(args),
        "aws-wait-rollout" => aws_rollout::run(args),
        "aws-plan-guard" => aws_simple::plan_guard(args),
        "aws-image-values" => aws_simple::image_values(args),
        "aws-deploy-values" => aws_simple::deploy_values(args),
        "aws-check-api-egress" => aws_simple::check_api_egress(args),
        "aws-check-dns-egress" => aws_simple::check_dns_egress(args),
        "aws-render-boundary" => aws_simple::render_boundary(args),
        "aws-rotate-secret" => aws_simple::rotate_secret(args),
        "aws-wait-checks" => aws_simple::wait_checks(args),
        "aws-verify" => aws_simple::verify_aws(args),
        _ => bail!("unknown operations subcommand: {command}"),
    }
    .with_context(|| format!("operations command failed: {command}"))
}
