class Dir(object):

    from utils.io import IO

    io = IO()

    # Directory for the prepocessed data subset
    PREPROCESSED = io.abs_path('res/data/processed')

    # EDA directory
    EDA = io.abs_path('res/report/eda')

    # Raw data
    RAW = io.abs_path('res/data/raw')

    # Google Maps library
    GMAPS = io.abs_path('res/lib/gmaps')

    # Public transportation library
    PUBLIC_TRANSPORT = io.abs_path('res/lib/public_transport')

    # Enriched data and layers
    ENRICHED = io.abs_path('res/data/enriched')


class File(object):

    from utils.io import IO

    io = IO()

    # Scrapped data
    SITEMAP = io.path_join(Dir.RAW, 'sitemap/sitemap.parquet')
    PAGES = io.path_join(Dir.RAW, 'pages/pages.parquet')
    CONTENT = io.path_join(Dir.RAW, 'content/content.parquet')

    # Subset of content that includes only relevant (filtered) entries
    SUBSET = io.abs_path('res/data/subset/data.parquet')

    # Points
    POINTS = io.abs_path('res/lib/points.json')

    # Google Maps
    ENTITIES_LIST = io.path_join(Dir.GMAPS, 'entities.json')
    EPICENTERS_LIST = io.path_join(Dir.GMAPS, 'epicenters.json')
    GMAPS_UNPROCESSED = io.path_join(Dir.GMAPS,  'unprocessed.parquet')
    GMAPS_PROCESSED = io.path_join(Dir.GMAPS, 'processed.parquet')

    # Public transportation
    SL_STOPS = io.path_join(Dir.PUBLIC_TRANSPORT, 'stops.json')
    SL_ROUTES = io.path_join(Dir.PUBLIC_TRANSPORT, 'routes.json')
    SL_PROCESSED = io.path_join(Dir.PUBLIC_TRANSPORT, 'sl.parquet')

    # Enrichment layers
    DESTINATIONS = io.path_join(Dir.ENRICHED, 'layers/destinations.parquet')
    ENTITIES = io.path_join(Dir.ENRICHED, 'layers/entities.parquet')
    TRANSPORT = io.path_join(Dir.ENRICHED, 'layers/transport.parquet')

    # Enriched subset
    ENRICHED_SUBSET = io.path_join(Dir.ENRICHED, 'data.parquet')

    # Preprocessed data
    X_TRAIN = io.path_join(Dir.PREPROCESSED, 'x_train')
    Y_TRAIN = io.path_join(Dir.PREPROCESSED, 'y_train')
    E_TRAIN = io.path_join(Dir.PREPROCESSED, 'e_train')

    X_VAL = io.path_join(Dir.PREPROCESSED, 'x_val')
    Y_VAL = io.path_join(Dir.PREPROCESSED, 'y_val')
    E_VAL = io.path_join(Dir.PREPROCESSED, 'e_val')

    X_TEST = io.path_join(Dir.PREPROCESSED, 'x_test')
    Y_TEST = io.path_join(Dir.PREPROCESSED, 'y_test')
    E_TEST = io.path_join(Dir.PREPROCESSED, 'e_test')

    # Features
    FEATURES_METADATA = io.abs_path('res/lib/features_metadata.csv')
    SELECTED_FEATURES = io.abs_path('res/model/features.txt')

    # Hyperparameters
    HYPERPARAMETERS = io.abs_path('res/model/bayesian_hyperparameters.json')

    # Model
    MODEL = io.abs_path('res/model/model.pkl')

    # Reports and illustrations
    PROPERTY_CLUSTERING_REPORT = io.abs_path('res/report/property_clustering.png')
    FEATURE_IMPORTANCE_REPORT = io.abs_path('res/report/feature_importance.png')

    # API keys
    API_KEYS = io.abs_path('secrets/api_keys.json')
