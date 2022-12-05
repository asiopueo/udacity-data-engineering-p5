class DataQualityChecks:
    quality_check_list = [
        {'sql': 'SELECT COUNT(*) FROM public.songplays',                       'type': 'gt', 'comparison': 0},
        {'sql': 'SELECT COUNT(*) FROM public.artists',                         'type': 'gt', 'comparison': 0},
        {'sql': 'SELECT COUNT(*) FROM public.songs',                           'type': 'gt', 'comparison': 0},
        {'sql': 'SELECT COUNT(*) FROM public.time',                            'type': 'gt', 'comparison': 0},
        {'sql': 'SELECT COUNT(*) FROM public.users',                           'type': 'gt', 'comparison': 0},
        {'sql': 'SELECT COUNT(*) FROM public.songplays WHERE playid IS NULL',  'type': 'eq', 'comparison': 0},
        {'sql': 'SELECT COUNT(*) FROM public.artists WHERE artistid IS NULL',  'type': 'eq', 'comparison': 0},
        {'sql': 'SELECT COUNT(*) FROM public.songs WHERE songid IS NULL',      'type': 'eq', 'comparison': 0},
        {'sql': 'SELECT COUNT(*) FROM public.time WHERE start_time IS NULL',   'type': 'eq', 'comparison': 0},
        {'sql': 'SELECT COUNT(*) FROM public.users WHERE userid IS NULL',      'type': 'eq', 'comparison': 0}
    ]