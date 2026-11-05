import pytest

from temperature_observability.config import Settings


def test_cost_uses_provisioned_capacity_and_one_shared_vm():
    settings = Settings(
        vm_hourly_cost_usd=0.0408,
        storage_provisioned_gb=20,
        storage_gb_monthly_cost_usd=0.088,
        executor_replicas=4,
    )
    assert settings.cost_configured
    assert settings.estimated_hourly_cost_usd * 730 == pytest.approx(0.0408 * 730 + 20 * 0.088)


def test_missing_storage_inputs_are_not_a_fully_configured_cost():
    assert not Settings(vm_hourly_cost_usd=0.0408).cost_configured
