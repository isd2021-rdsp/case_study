from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules


def get_rules(ds, min_support, max_len, target, metric='lift', min_threshold=1):
    frequent_items = apriori(ds, use_colnames=True,
                             min_support=min_support, max_len=max_len)

    rules = association_rules(frequent_items, metric=metric, min_threshold=min_threshold)

    return rules[rules['consequents'].astype(str) == target].sort_values(by='confidence', ascending=False)