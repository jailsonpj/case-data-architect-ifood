from authentication.authentication import AuthenticationS3
import urllib

auth = eval(AuthenticationS3.get_secret_value())

access_key = auth["user-dl-case-ifood"]
secret_key = auth["secret-dl-case-ifood"]
encoded_secret = urllib.parse.quote(string=secret_key, safe="")

aws_s3_bucket_raw = "raw-tlc-trip-data-case-ifood"
mount_name_raw = "/mnt/mount_raw"
source_url_raw = "s3a://%s:%s@%s" %(access_key, encoded_secret, aws_s3_bucket_raw)

aws_s3_bucket_transformed = "transformed-tlc-trip-data-case-ifood"
mount_name_transformed = "/mnt/mount_transformed"
source_url_transformed = "s3a://%s:%s@%s" %(access_key, encoded_secret, aws_s3_bucket_transformed)

aws_s3_bucket_enriched = "enriched-tlc-trip-data-case-ifood"
mount_name_enriched = "/mnt/mount_enriched"
source_url_enriched = "s3a://%s:%s@%s" %(access_key, encoded_secret, aws_s3_bucket_enriched)