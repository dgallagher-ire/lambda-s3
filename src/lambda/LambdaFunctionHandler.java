package lambda;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3BucketEntity;
import com.amazonaws.services.s3.event.S3EventNotification.S3Entity;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.event.S3EventNotification.S3ObjectEntity;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	@Override
	public String handleRequest(S3Event input, Context context) {
		final LambdaLogger logger = context.getLogger();
		try {
			final List<S3EventNotificationRecord> records = input.getRecords();
			for (final S3EventNotificationRecord record : records) {
				this.printDetails(logger, record);
			}
		} catch (Exception e) {
			logger.log(e.toString());
		}
		return "done";
	}

	private void printDetails(final LambdaLogger logger, final S3EventNotificationRecord record) throws Exception {

		final String region = record.getAwsRegion();
		final String eventName = record.getEventName();
		final String eventSource = record.getEventSource();

		final S3Entity entity = record.getS3();
		final S3ObjectEntity object = entity.getObject();
		final String key = object.getKey();
		final Long size = object.getSizeAsLong();

		final S3BucketEntity bucket = entity.getBucket();
		final String bucketName = bucket.getName();

		final String contents = this.readS3Contents(logger, bucketName, key);

		logger.log("Region:\t" + region);
		logger.log("EventName:\t" + eventName);
		logger.log("EventSource:\t" + eventSource);
		logger.log("Key:\t" + key);
		logger.log("Size:\t" + size);
		logger.log("Bucket:\t" + bucketName);
		logger.log("Contents:\t" + contents);
	}

	private void copyContentsToFile(final LambdaLogger logger,
			final AmazonS3 s3Client,
			final String bucketName,
			final String key,
			final String contents) throws Exception {
		
		try {
			final String newFileName = key.replace(".dg", ".copy");
			s3Client.putObject(bucketName, newFileName, contents);
		} catch (Exception e) {
			logger.log(e.toString());
			throw e;
		}
	}

	private String readS3Contents(final LambdaLogger logger, final String bucketName, final String key)
			throws Exception {

		InputStream in = null;
		try {
			final AmazonS3 s3Client = new AmazonS3Client();
			final GetObjectRequest objectRequest = new GetObjectRequest(bucketName, key);
			final S3Object object = s3Client.getObject(objectRequest);
			in = object.getObjectContent();
			final InputStreamReader inReader = new InputStreamReader(in);
			final BufferedReader reader = new BufferedReader(inReader);
			final StringBuilder sb = new StringBuilder();
			String line;
			while (true) {
				line = reader.readLine();
				if (line == null) {
					break;
				}
				sb.append(line).append("\r\n");
			}
			return sb.toString();
		} catch (Exception e) {
			logger.log(e.toString());
			throw e;
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}

}
