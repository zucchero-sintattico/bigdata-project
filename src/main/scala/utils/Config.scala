package utils

object Config {

  // The local directory containing this repository
  val projectDir = "/Users/tbrin/Desktop/bigdata-project/"
  //val projectDir: String = "/Users/giggino/Desktop/bigdata-project.nosync"
  // The name of the shared bucket on AWS S3 to read datasets
  val s3sharedBucketName: String = "unibo-bd2425-egallinucci" // TODO: change this
  // The name of your bucket on AWS S3
  val s3bucketName: String = "..."
  // The path to the credentials file for AWS (if you follow instructions, this should not be updated)
  val credentialsPath: String = "/aws_credentials.txt" // TODO: check this

}
