package com.ctrip.hermes.consumer.pull;

import java.util.List;

import com.ctrip.hermes.core.message.ConsumerMessage;

public interface Committer<T> {

	RetriveSnapshot<T> delivered(List<ConsumerMessage<T>> msgs);

	void scanAndCommitAsync();

	void close();

}
