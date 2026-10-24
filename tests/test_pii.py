"""El hash de PII es determinístico con el mismo salt y cambia con otro salt."""

import pandas as pd

from steam_lib.pii import hash_series, hash_value

# Valores de prueba: no tienen relación con el PII_SALT real.
SALT_A = "salt-de-test-a"
SALT_B = "salt-de-test-b"


def test_mismo_valor_y_mismo_salt_da_el_mismo_hash():
    assert hash_value("gamer@example.com", SALT_A) == hash_value("gamer@example.com", SALT_A)
    assert len(hash_value("gamer@example.com", SALT_A)) == 64  # SHA-256 en hexadecimal


def test_otro_salt_da_otro_hash():
    assert hash_value("gamer@example.com", SALT_A) != hash_value("gamer@example.com", SALT_B)


def test_se_normaliza_antes_de_hashear_y_los_nulos_no_se_hashean():
    hashes = hash_series(pd.Series([" Gamer@Example.COM ", "gamer@example.com", "", None]), SALT_A)
    assert hashes[0] == hashes[1] == hash_value("gamer@example.com", SALT_A)
    assert hashes[2:].isna().all()
