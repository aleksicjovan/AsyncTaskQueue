//
//  TQQueue.swift
//  AsynchTaskQueue
//
//  Created by joca on 6/29/19.
//  Copyright © 2019 joca. All rights reserved.
//

import Foundation
import CouchbaseLiteSwift

public struct TQTaskDependancyMap {

	let type: AnyClass?

	let dependancyReferenceList: [String]?

	public init(_ type: AnyClass?, _ dependancyReferenceList: [String]?) {
		self.type = type
		self.dependancyReferenceList = dependancyReferenceList
	}
}

public final class TQQueue: TQMonitor {

	public let name: String

	public let threadNumber: Int

	private var threads = [Thread]()

	public func addTask(_ task: TQTask, taskDependancyMap: [TQTaskDependancyMap]?) -> Bool {
		task.queueName = name
		return TQQueueManager.shared.addTask(task, taskDependancyMap: taskDependancyMap)
	}

	internal func getNextReadyTask() -> TQTask? {
		var task: TQTask?
		synchronized {
			task = TQQueueManager.shared.getNextReadyTask(for: name)
		}
		return task
	}

	internal func taskFailed(_ task: TQTask, error: Error) -> Bool {
		return TQQueueManager.shared.taskFailed(task)
	}

	internal func taskSucceeded(_ task: TQTask) {
		TQQueueManager.shared.taskSucceeded(task)
	}

	public func startThreads() {
		synchronized {
			threads.removeAll(where: { !$0.isExecuting })
			let numberOfThreadsToStart = threadNumber - threads.count
			for _ in 1...numberOfThreadsToStart {
				let thread = TQThread(queue: self)
				threads.append(TQThread(queue: self))
				thread.start()
			}
		}
	}

	public func stopThreads() {
		synchronized {
			for thread in threads {
				if thread.isExecuting {
					thread.cancel()
				}
			}
			threads = []
		}
	}

	internal init(name: String, threadNumber: Int) {
		self.name = name
		self.threadNumber = threadNumber

		super.init()
	}

}
