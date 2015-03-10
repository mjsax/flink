package org.apache.flink.streaming.io;

import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.apache.flink.streaming.api.streamrecord.OrderedStreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * TODO
 * 
 *
 * @author mjsax
 *
 * @param <IN>
 */
public class MergeBuffer {
	private static final Logger LOG = LoggerFactory.getLogger(MergeBuffer.class);

	/**
	 * TODO
	 */
	private final LinkedList<OrderedStreamRecord<?>>[] channelBuffer;
	/**
	 * TODO
	 */
	private long latestTs = Long.MIN_VALUE;
	
	
	/**
	 * 
	 * @param numberOfInputChannels
	 */
	@SuppressWarnings("unchecked")
	public MergeBuffer(final int numberOfInputChannels) {
		assert(numberOfInputChannels >= 0);
		
		this.channelBuffer = new LinkedList[numberOfInputChannels];
		for(int i = 0; i < numberOfInputChannels; ++i) {
			this.channelBuffer[i] = new LinkedList<OrderedStreamRecord<?>>();
		}
	}
	
	
	
	/**
	 * 
	 */
	public void addTuple(OrderedStreamRecord<?> record, int index) {
		assert(record != null);
		this.channelBuffer[index].add(record);
	}
	
	/**
	 * 
	 */
	public OrderedStreamRecord<?> getNext() {
		long minTsFound = Long.MAX_VALUE;
		boolean eachBufferFilled = true;
		int minTsPartitionNumber = -1;
		
		for(int i = 0; i < this.channelBuffer.length; ++i) {
			try {
				long ts = this.channelBuffer[i].getFirst().getTs();
				assert (ts >= this.latestTs);
				
				if(ts == this.latestTs) {
					LOG.trace("Extract tuple with same timestamp (input channel, tuple): {}, {}",
						new Integer(i), this.channelBuffer[i].getFirst());
					return this.channelBuffer[i].removeFirst();
				}
				
				if(ts < minTsFound) {
					minTsFound = ts;
					minTsPartitionNumber = i;
				}
			} catch(NoSuchElementException e) {
				// in this case, we stay in the loop, because we still might find a tuple with equal ts value as last
				// returned tuple
				LOG.trace("Found empty input channel: {}", new Integer(i));
				eachBufferFilled = false;
			}
		}
		
		if(eachBufferFilled && minTsPartitionNumber != -1) {
			LOG.trace("Extract tuple min timestamp (ts, input channel, tuple): {}, {}", minTsFound,
				minTsPartitionNumber, this.channelBuffer[minTsPartitionNumber].getFirst());
			this.latestTs = minTsFound;
			return this.channelBuffer[minTsPartitionNumber].removeFirst();
		}
		
		LOG.trace("Could not extract record.");
		return null;
	}
	
}
