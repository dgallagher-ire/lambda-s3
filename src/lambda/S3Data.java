package lambda;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.event.S3EventNotification.S3BucketEntity;
import com.amazonaws.services.s3.event.S3EventNotification.S3Entity;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.event.S3EventNotification.S3ObjectEntity;

public class S3Data {

	private final String region;
	private final String eventName;
	private final String eventSource;
	private final String key;
	private final Long size;
	private final String tag;
	private final String version;
	private final String bucketName;
	private String contents;

	public S3Data(final S3EventNotificationRecord record) {
		this.region = record.getAwsRegion();
		this.eventName = record.getEventName();
		this.eventSource = record.getEventSource();
		final S3Entity entity = record.getS3();
		final S3ObjectEntity object = entity.getObject();
		this.key = object.getKey();
		this.size = object.getSizeAsLong();
		this.tag = object.geteTag();
		this.version = object.getVersionId();

		final S3BucketEntity bucket = entity.getBucket();
		this.bucketName = bucket.getName();
	}

	public final void printData(final LambdaLogger logger) {
		logger.log("Region:\t" + this.getRegion());
		logger.log("EventName:\t" + this.getEventName());
		logger.log("EventSource:\t" + this.getEventSource());
		logger.log("Key:\t" + this.getKey());
		logger.log("Size:\t" + this.getSize());
		logger.log("Tag:\t" + this.getTag());
		logger.log("Version:\t" + this.getVersion());
		logger.log("Bucket:\t" + this.getBucketName());
		logger.log("Contents:\t" + this.getContents());
	}

	public final String getContents() {
		return contents;
	}

	public final void setContents(String contents) {
		this.contents = contents;
	}

	public final String getRegion() {
		return region;
	}

	public final String getEventName() {
		return eventName;
	}

	public final String getEventSource() {
		return eventSource;
	}

	public final String getKey() {
		return key;
	}

	public final Long getSize() {
		return size;
	}

	public final String getTag() {
		return tag;
	}

	public final String getVersion() {
		return version;
	}

	public final String getBucketName() {
		return bucketName;
	}

}
