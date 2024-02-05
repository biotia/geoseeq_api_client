import pandas as pd
from hashlib import md5

COLORS = [
    "#0057FF",
    "#FF815A",
    "#42E282",
    "#A855F7",

    "#6698FF",
    "#B72B00",
    "#7BEDA5",
    "#7E22CE",

    "#D0DFFF",
    "#5B1500",
    "#D4FCE2",
    "#581C87",

    "#FFDBD0",
    "#14532D",
    "#F9F3FF",

    "#FFA185",
    "#268248",
    "#D8B4FE"
]


def unique_values_to_colors(values):
    """Map a set of unique values to colors in a stable way.
    
    Rules:
    the same set of values should always map to the same colors
    no colors should be used twice unless there are more values than colors
    two sets of values with overlapping elemnts should try to map those elements to the same colors
    """
    values_to_hashes, used_hashes = {}, set()
    for value in values:
        hash_val = int(md5(str(value).encode('utf-8')).hexdigest(), 16) % len(COLORS)
        if len(used_hashes) < len(COLORS):
            while hash_val in used_hashes:
                hash_val = (hash_val + 1) % len(COLORS)
            used_hashes.add(hash_val)
        values_to_hashes[value] = hash_val
    return {value: COLORS[hash_val] for value, hash_val in values_to_hashes.items()}


def values_to_colors(values):
    """Map a non-unique set of values to colors in a stable way."""
    unique_values = {str(value) for value in values}
    color_map = unique_values_to_colors(unique_values)
    return [color_map[str(value)] for value in values]


def float_or_nan(x):
    """Convert a value to float or return np.nan if it can't be converted."""
    try:
        return float(x)
    except ValueError:
        return float('nan')
    except TypeError:
        return float('nan')
    

def col_is_numeric(col, min_unique=3, min_fill=0.1):
    """Return True iff a pandas series is numeric. If numeric also return the cast series else None.
    
    Numeric according to these criteria"
    1. The column can be converted into float
    2. The column has at least 6 unique values
    3. The column has at least 10% of the rows with a value that is not null
    """
    numeric_col = col.apply(float_or_nan)
    if numeric_col.nunique() >= min_unique:
        if numeric_col.count() / col.shape[0] >= min_fill:
            return True, numeric_col
    return False, None


def col_is_categorical(col, max_unique=20, min_unique=2, min_fill=0.1):
    """Return True iff a pandas series is categorical. If categorical also return the cast series else None.
    
    Categroical according to these criteria"
    1. The column can be converted into string
    2. The column has lte 20 unique values
    3. The column has at least 2 unique values
    4. The column has at least 10% of the rows with a value that is not null
    5. The column is not numeric according to col_is_numeric with default params
    """
    val_col = col.apply(str)
    if col.nunique() <= max_unique:
        if col.nunique() >= min_unique:
            if col.count() / col.shape[0] >= min_fill:
                if not col_is_numeric(col)[0]:
                    # val_col = pd.Categorical(val_col, categories=sorted(val_col.unique()))
                    return True, val_col
    return False, None

