from bec_lib.utils.copier_jinja_filters import _snake_to_camel, _snake_to_pascal


def test_camel_case():
    assert _snake_to_camel("test_string") == "testString"
    assert _snake_to_camel("test_string_2") == "testString2"
    assert _snake_to_camel("test_strIng_wITH_CAPS") == "testStringWithCaps"
    assert _snake_to_camel("TestStringAlreadyPascal") == "teststringalreadypascal"


def test_pascal_case():
    assert _snake_to_pascal("test_string") == "TestString"
    assert _snake_to_pascal("test_string_2") == "TestString2"
    assert _snake_to_pascal("test_strIng_wITH_CAPS") == "TestStringWithCaps"
    assert _snake_to_pascal("testStringAlreadyCamel") == "Teststringalreadycamel"
    assert _snake_to_pascal("t_e_s_t") == "TEST"
