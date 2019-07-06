//
//  TQQueue.swift
//  AsynchTaskQueue
//
//  Created by joca on 6/29/19.
//  Copyright © 2019 joca. All rights reserved.
//

import Foundation

public struct TQTaskDependancyMap {

	let type: AnyClass?

	let dependancyReferenceList: [String]?

	public init(_ type: AnyClass?, _ dependancyReferenceList: [String]?) {
		self.type = type
		self.dependancyReferenceList = dependancyReferenceList
	}
}

public final class TQQueue {

	private(set) var initialized = false

	private(set) var mainBundle: Bundle!

	private var taskDatabase = TQTaskDatabase()

	private var threads = [TQThread]()

	private var currentThread: Thread?

	private let synchronizationSemaphore = DispatchSemaphore(value: 1)

	private func synchronized(_ block: () -> Void) {
		if currentThread != Thread.current {
			synchronizationSemaphore.wait()
			currentThread = Thread.current
		}

		defer {
			currentThread = nil
			synchronizationSemaphore.signal()
		}

		block()
	}

	internal func getNextTask() -> TQTask? {
		var nextTask: TQTask? = nil

		synchronized {
			nextTask = taskDatabase.getFirstReadyTask()

			if nextTask != nil {
				nextTask!.state = .running
				taskDatabase.saveTask(nextTask!)
			}
		}

		return nextTask
	}

	private func moveToEndOfQueue(_ task: TQTask) {
		synchronized {
			task.additionTimestamp = Date().timeIntervalSince1970
			taskDatabase.saveTask(task)
		}
	}

	internal func taskFinished(_ task: TQTask, error: Error?) -> Bool {
		var rerunNow = false

		synchronized {
			if error != nil {
				task.retryCounter += 1
				task.totalTryCounter += 1

				if task.totalTryCounter > task.maxNumberOfTries {
					taskDatabase.removeTask(task)
				} else if task.retryCounter > task.maxNumberOfRetries {
					task.retryCounter = 0
					moveToEndOfQueue(task)
				} else {
					taskDatabase.saveTask(task)
					rerunNow = true
				}
			} else {
				taskDatabase.updateAllDependentTasks(task)
				taskDatabase.removeTask(task)
			}
		}

		return rerunNow
	}

	public func addTask(_ task: TQTask, taskDependancyMap: [TQTaskDependancyMap]? = nil) -> Bool {
		if initialized {
			synchronized {
				let dependencyList = taskDatabase.getDependencyList(for: taskDependancyMap)
				task.dependencyList = dependencyList
				if dependencyList.count > 0 {
					task.state = .notReady
				}
				taskDatabase.addTask(task)
//				startThreads()
			}
			return true
		} else {
			return false
		}
	}

	public func startThreads() {
		synchronized {
			for thread in threads {
				if !thread.isExecuting {
					thread.start()
				}
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
		}
	}

	private init() {
		for _ in 1...TQConfig.NUMBER_OF_THREADS {
			threads.append(TQThread())
		}
	}

	public func initialize(key: String, mainBundle bundle: Bundle) -> Bool {
		synchronized {
			mainBundle = bundle
			initialized = taskDatabase.initialize(databaseKey: key)
			if initialized {
//				startThreads()
			}
		}
		return initialized
	}

	public func uninitialize() {
		synchronized {
			stopThreads()
			taskDatabase.uninitialize()
			initialized = false
		}
	}

	public static let shared = TQQueue()

}
