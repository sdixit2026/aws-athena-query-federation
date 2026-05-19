# Athena PostgreSQL Connector — Deployment & Debugging Guide

## Overview

This connector enables Amazon Athena to query PostgreSQL databases via a Lambda function.
There are two deployment variants:

| Template | Handler | Connection Method |
|---|---|---|
| `athena-postgresql-package.yaml` | JAR-based, java11 | `DefaultConnectionString` env var |
| `athena-postgresql-connection.yaml` | Docker image | `glue_connection` env var (reads from AWS Glue) |

Use `athena-postgresql-connection.yaml` if your connection details are stored in a Glue connection.
Use `athena-postgresql-package.yaml` for direct JDBC connection string deployment or SAR publishing.

---

## Prerequisites

- AWS CLI configured (`aws configure`)
- SAM CLI installed (`brew install aws-sam-cli`)
- Docker installed and running
- Maven 3.x + Java 11
- An S3 bucket for SAM artifacts (e.g. `sam-artifacts-pg-connector`)
- An ECR repository for the Docker image (only for `athena-postgresql-connection.yaml`)

---

## End-to-End Deployment (Glue Connection variant)

### Step 1 — Build the JAR

```bash
cd athena-postgresql
mvn clean package -DskipTests
```

Produces `target/athena-postgresql-2022.47.1.jar`.

### Step 2 — Create ECR Repository (first time only)

```bash
aws ecr create-repository \
  --repository-name athena-postgresql-connector \
  --region <your-region>
```

### Step 3 — Build and Push Docker Image

**Important:** Always build with `--platform linux/amd64` and `--provenance=false`.
Building on Apple Silicon (M1/M2/M3) without these flags produces an arm64 or OCI manifest image
that Lambda rejects.

```bash
# Authenticate to ECR
aws ecr get-login-password --region <your-region> | \
  docker login --username AWS --password-stdin <account-id>.dkr.ecr.<your-region>.amazonaws.com

# Build for the correct platform
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  --push \
  -t <account-id>.dkr.ecr.<your-region>.amazonaws.com/athena-postgresql-connector:latest \
  .
```

### Step 4 — Update Template ImageUri

In `athena-postgresql-connection.yaml`, set `ImageUri` to your ECR image (not AWS's):

```yaml
Resources:
  JdbcConnectorConfig:
    Properties:
      PackageType: "Image"
      ImageUri: '<account-id>.dkr.ecr.<your-region>.amazonaws.com/athena-postgresql-connector:latest'
```

### Step 5 — SAM Package

```bash
sam package \
  --template-file athena-postgresql-connection.yaml \
  --output-template-file packaged.yaml \
  --image-repository <account-id>.dkr.ecr.<your-region>.amazonaws.com/athena-postgresql-connector \
  --s3-bucket sam-artifacts-pg-connector \
  --region <your-region>
```

Both flags are required:
- `--image-repository` — where the Docker image is pushed
- `--s3-bucket` — where `README.md` and `LICENSE.txt` are uploaded for SAR metadata

### Step 6 — SAM Publish (to Serverless Application Repository)

```bash
sam publish \
  --template packaged.yaml \
  --region <your-region>
```

Output:
```
Successfully published application: arn:aws:serverlessrepo:<region>:<account-id>:applications/AthenaPostgreSQLConnectorWithGlueConnection
```

Save this ARN. Find the app under **AWS Console → Serverless Application Repository → Private applications**.

### Step 7 — Deploy (SAM Deploy)

```bash
sam deploy \
  --template-file packaged.yaml \
  --stack-name athena-postgres-glue-connector \
  --capabilities CAPABILITY_IAM CAPABILITY_RESOURCE_POLICY \
  --region <your-region> \
  --parameter-overrides \
    LambdaFunctionName=athena-postgres-connector \
    GlueConnection=<glue-connection-name> \
    SecretName=<secret-name-prefix> \
    SpillBucket=sam-artifacts-pg-connector \
    SecurityGroupIds=sg-xxxxxxxx \
    SubnetIds=subnet-xxxxxxxx
```

### Step 8 — Grant ECR Access to Lambda

After the Lambda is created, ECR needs a resource policy so Lambda can pull the image:

```bash
aws ecr set-repository-policy \
  --repository-name athena-postgresql-connector \
  --region <your-region> \
  --policy-text '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "LambdaECRAccess",
        "Effect": "Allow",
        "Principal": {
          "Service": "lambda.amazonaws.com"
        },
        "Action": [
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer"
        ]
      }
    ]
  }'
```

### Step 9 — Register as Athena Data Source

1. Go to **Athena console → Data sources → Connect data source**
2. Choose **Lambda function**
3. Select your Lambda: `athena-postgres-connector`
4. Set a catalog name, e.g. `my_postgres`

### Step 10 — Query

```sql
SELECT * FROM "my_postgres"."public"."your_table" LIMIT 10;
```

---

## Publishing a New Version

Bump `SemanticVersion` in `athena-postgresql-connection.yaml`, then repeat Steps 3–6.
SAR does not allow re-publishing the same version number.

```yaml
SemanticVersion: 1.0.2   # increment each publish
```

---

## EventBridge Warm-Up (Keep Lambda Warm)

The connector handles EventBridge scheduled events to avoid cold starts.
When EventBridge invokes the Lambda, it sends:

```json
{
  "source": "aws.events",
  "detail-type": "Scheduled Event",
  "detail": {}
}
```

The handler detects `"source": "aws.events"` and returns immediately without attempting
Athena deserialization. To set up the warm-up rule:

```bash
aws events put-rule \
  --name athena-postgres-warmup \
  --schedule-expression "rate(5 minutes)" \
  --state ENABLED

aws events put-targets \
  --rule athena-postgres-warmup \
  --targets "Id=1,Arn=arn:aws:lambda:<region>:<account>:function:athena-postgres-connector"

aws lambda add-permission \
  --function-name athena-postgres-connector \
  --statement-id eventbridge-warmup \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:<region>:<account>:rule/athena-postgres-warmup
```

---

## Debugging Common Errors

### `Source image does not exist`

```
Source image 292517598671.dkr.ecr... does not exist
```

**Cause:** `ImageUri` in the template still points to AWS's ECR account, not yours.

**Fix:** Update `ImageUri` in `athena-postgresql-connection.yaml` to your ECR URI, then re-run
`sam package` and `sam publish`.

---

### `Image manifest media type not supported`

```
The image manifest, config or layer media type for the source image is not supported
```

**Cause:** Image was built on Apple Silicon without specifying platform, producing an `arm64`
or OCI manifest list that Lambda rejects.

**Fix:** Rebuild with:
```bash
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  --push \
  -t <account-id>.dkr.ecr.<region>.amazonaws.com/athena-postgresql-connector:latest \
  .
```

---

### `Lambda cannot access ECR image (403)`

```
Lambda cannot access the Amazon ECR image. Ensure the ECR repository policy has
ecr:BatchGetImage and ecr:GetDownloadUrlForLayer permissions.
```

**Cause:** ECR repository lacks a resource policy allowing Lambda to pull the image.

**Fix:** Run the `aws ecr set-repository-policy` command from Step 8 above.

---

### `S3 Bucket not specified` during `sam package`

```
S3 Bucket not specified, use --s3-bucket to specify a bucket name
```

**Cause:** `sam package` needs `--s3-bucket` to upload `README.md` and `LICENSE.txt`
referenced in the SAR metadata, even for image-based functions.

**Fix:** Add `--s3-bucket sam-artifacts-pg-connector` alongside `--image-repository`.

---

### `Usage: sam package [OPTIONS]` with no detail

**Cause:** The template uses `PackageType: "Image"` but only `--s3-bucket` was passed.
Image-based functions require `--image-repository`.

**Fix:** Use both flags together (see Step 5).

---

### CloudFormation deploys old image after republish

**Cause:** `sam publish` was run before `sam package` regenerated `packaged.yaml` with the
updated `ImageUri`, so SAR stored the old template.

**Fix:** Bump `SemanticVersion`, re-run `sam package`, then `sam publish`. When deploying
from SAR, explicitly select the new version.

---

### WARN: `Unmapped JDBC type: 1111` / `defaulting type to VARCHAR`

```
WARN JdbcArrowTypeConverter - Error converting JDBC Type [1111] to arrow: Unmapped JDBC type: 1111
WARN JdbcMetadataHandler - getSchema: Unable to map type for column[prsheadline] to a supported type
```

**Cause:** JDBC type `1111` is `Types.OTHER` — used by the PostgreSQL JDBC driver for
PostgreSQL-specific types with no standard SQL equivalent: `tsvector`, `tsquery`, `anyarray`,
`hstore`, etc. Columns like `prsheadline` (tsvector) or `most_common_vals`, `histogram_bounds`,
`most_common_elems` (anyarray, from `pg_stats` system catalog) trigger this.

**Impact:** None. The connector correctly falls back to `VARCHAR`. Data is returned as its
string representation. Queries continue to work.

**Fix:** None required. If you want to silence the log, add an explicit mapping in
`athena-jdbc/src/main/java/com/amazonaws/athena/connectors/jdbc/manager/JdbcArrowTypeConverter.java`:

```java
if (jdbcType == Types.OTHER) {
    return Optional.of(new ArrowType.Utf8());
}
```

---

## Connection String Format (non-Glue variant)

```
postgres://jdbc:postgresql://<host>:<port>/<database>?user=<user>&password=<pass>
```

With Secrets Manager:
```
postgres://jdbc:postgresql://<host>:<port>/<database>?${secret_name}
```

---

## Handler Reference

| Class | Use case |
|---|---|
| `PostGreSqlCompositeHandler` | Single PostgreSQL instance, uses `DefaultConnectionString` |
| `PostGreSqlMuxCompositeHandler` | Multiple PostgreSQL instances, uses catalog-specific connection strings |
