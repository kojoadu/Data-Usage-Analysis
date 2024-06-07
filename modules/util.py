def formatIndex(df):
    df['S/No.'] = range(1, len(df) + 1)
    df = df.set_index('S/No.')
    return df


def get_min_date(fbb_df):
    return fbb_df['Start_Time'].min()


def get_max_date(fbb_df):
    return fbb_df['Start_Time'].max()

# CSS for modernizing the page
