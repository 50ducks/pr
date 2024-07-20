#primary key dictionary

pk_dict = {
    "ds.ft_balance_f":["ON_DATE", "ACCOUNT_RK"],
    "ds.ft_posting_f":"none",
    "ds.md_account_d":["DATA_ACTUAL_DATE", "ACCOUNT_RK"],
    "ds.md_currency_d":["CURRENCY_RK", "DATA_ACTUAL_DATE"],
    "ds.md_exchange_rate_d":["DATA_ACTUAL_DATE", "CURRENCY_RK"],
    "ds.md_ledger_account_s":["LEDGER_ACCOUNT", "START_DATE"]
}