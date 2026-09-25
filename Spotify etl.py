import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node track
track_node1790187075818 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-etl-project-18-09-26/staging/spotify_tracks_data_2023.csv"], "recurse": True}, transformation_ctx="track_node1790187075818")

# Script generated for node album
album_node1790187061683 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-etl-project-18-09-26/staging/spotify-albums_data_2023.csv"], "recurse": True}, transformation_ctx="album_node1790187061683")

# Script generated for node artist
artist_node1790187074751 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-etl-project-18-09-26/staging/spotify_artist_data_2023.csv"], "recurse": True}, transformation_ctx="artist_node1790187074751")

# Script generated for node Drop Fields
DropFields_node1790238139763 = DropFields.apply(frame=track_node1790187075818, paths=["explicit"], transformation_ctx="DropFields_node1790238139763")

# Script generated for node Drop Fields
DropFields_node1790238726743 = DropFields.apply(frame=album_node1790187061683, paths=["artists", "artist_7", "artist_8", "artist_9", "artist_10", "artist_11", "artist_6"], transformation_ctx="DropFields_node1790238726743")

# Script generated for node Drop Fields
DropFields_node1790238890900 = DropFields.apply(frame=artist_node1790187074751, paths=["genre_5", "genre_6", "genre_4"], transformation_ctx="DropFields_node1790238890900")

# Script generated for node Join album & artist
Joinalbumartist_node1790238960346 = Join.apply(frame1=DropFields_node1790238726743, frame2=DropFields_node1790238890900, keys1=["artist_id"], keys2=["id"], transformation_ctx="Joinalbumartist_node1790238960346")

# Script generated for node Join with track
Joinwithtrack_node1790239207954 = Join.apply(frame1=DropFields_node1790238139763, frame2=Joinalbumartist_node1790238960346, keys1=["id"], keys2=["track_id"], transformation_ctx="Joinwithtrack_node1790239207954")

# Script generated for node Drop Fields
DropFields_node1790240059406 = DropFields.apply(frame=Joinwithtrack_node1790239207954, paths=["`.id`"], transformation_ctx="DropFields_node1790240059406")

# Script generated for node S3 destination
EvaluateDataQuality().process_rows(frame=DropFields_node1790240059406, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790227605435", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
S3destination_node1790240356667 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1790240059406, connection_type="s3", format="glueparquet", connection_options={"path": "s3://spotify-etl-project-18-09-26/data warehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3destination_node1790240356667")

job.commit()