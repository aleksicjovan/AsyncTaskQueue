//
//  TQQueue.swift
//  AsynchTaskQueue
//
//  Created by joca on 6/29/19.
//  Copyright © 2019 joca. All rights reserved.
//

import Foundation

public final class TQQueue {

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

	public func taskFinished(_ task: TQTask, error: Error?) -> Bool {
		var rerunNow = false

		synchronized {
			if error != nil {
				task.retryCounter += 1
				task.totalTryCounter += 1
				taskDatabase.saveTask(task)

				if task.totalTryCounter > task.maxNumberOfTries {
					taskDatabase.removeTask(task)
				} else if task.retryCounter > task.maxNumberOfRetries {
					taskDatabase.moveToEndOfQueue(task)
				} else {
					rerunNow = true
				}
			} else {
				taskDatabase.removeTask(task)
			}
		}

		return rerunNow
	}

	public func addTask(_ task: TQTask) {
		synchronized {
			taskDatabase.addTask(task)
			// startThreads()
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

	private init() {
		for _ in 1...TQConfig.NUMBER_OF_THREADS {
			threads.append(TQThread())
		}
	}

	public static let shared = TQQueue()

}
