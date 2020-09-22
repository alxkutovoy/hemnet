from enum import Enum


class Dir(Enum):
    # Directory for the prepocessed data subset
    PREPROCESSED = 'data/dataset/processed'
    EDA = 'data/eda'


class File(object):
    # Scrapped data
    SITEMAP = 'data/sitemap/sitemap.parquet'
    PAGES = 'data/pages/pages.parquet'
    CONTENT = 'data/content/content.parquet'
    # Subset of content that includes only relevant (filtered) entries
    SUBSET = 'data/dataset/raw/data.parquet'
    # Google maps
    POINTS = 'resource/points.json'
    ENTITIES = 'resource/entities.json'
    EPICENTERS = 'resource/epicenters.json'
    GMAPS_UNPROCESSED = 'data/library/gmaps/unprocessed.parquet'
    GMAPS_PROCESSED = 'data/library/gmaps/processed.parquet'
    # Public transportation
    SL_STOPS = 'data/library/public_transport/stops.json'
    SL_ROUTES = 'data/library/public_transport/routes.json'
    SL_PROCESSED = 'data/library/public_transport/sl.parquet'
    # Enrichment layers
    DESTINATIONS = 'data/dataset/features/destinations.parquet'
    ENTITIES_PATH = 'data/dataset/features/entities.parquet'  # TODO: Rename
    TRANSPORT = 'data/dataset/features/transport.parquet'
    # Enriched subset
    ENRICHED_SUBSET = 'data/dataset/enriched/data.parquet'
    # Preprocessed data
    X_TRAIN = 'data/dataset/processed/x_train'
    Y_TRAIN = 'data/dataset/processed/y_train'
    E_TRAIN = 'data/dataset/processed/e_train'
    X_VAL = 'data/dataset/processed/x_val'
    Y_VAL = 'data/dataset/processed/y_val'
    E_VAL = 'data/dataset/processed/e_val'
    X_TEST = 'data/dataset/processed/x_test'
    Y_TEST = 'data/dataset/processed/y_test'
    E_TEST = 'data/dataset/processed/e_test'
    # Features
    FEATURES_METADATA = 'resource/features_metadata.csv'
    SELECTED_FEATURES = 'data/dataset/processed/operations/features.txt'  # Results of the feature selection step
    # Hyperparameters
    HYPERPARAMETERS = 'data/dataset/processed/operations/bayesian_hyperparameters.json'
    # Model
    MODEL = 'output/model/model.pkl'
    # Reports and illustrations
    PROPERTY_CLUSTERING_REPORT = 'data/dataset/reporting/property_clustering.png'
    FEATURE_IMPORTANCE_REPORT = 'data/dataset/reporting/feature_importance.png'
    EDA_REPORT = 'data/eda/data_profiling_report.html'
