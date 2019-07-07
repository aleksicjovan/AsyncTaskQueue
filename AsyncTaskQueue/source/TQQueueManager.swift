//
//  TQQueueManager.swift
//  AsyncTaskQueue
//
//  Created by joca on 7/6/19.
//  Copyright © 2019 joca. All rights reserved.
//

import Foundation

public final class TQQueueManager: TQMonitor {

	public let taskFailedNotificationName = Notification.Name("taskFailed")

	public let taskSucceededNotificationName = Notification.Name("taskSucceeded")

	public private(set) var initialized = false

	internal private(set) var mainBundle: Bundle!

	private var queues = [TQQueue]()

	private var helperDispatchQueue = DispatchQueue.init(label: "helper")

	private var taskDatabase = TQTaskDatabase()

	private func moveToEndOfQueue(_ task: TQTask) {
		synchronized {
			task.additionTimestamp = Date().timeIntervalSince1970
			taskDatabase.saveTask(task)
		}
	}

	private func updateAllTasks(dependingOn task: TQTask) {
		let dependentTasks = taskDatabase.getAllTasks(dependingOn: task)

		for dependentTask in dependentTasks {
			dependentTask.dependencyList.removeAll(where: { $0 == task.id })
			if dependentTask.dependencyList.count == 0 {
				dependentTask.state = .ready
			}
		}

		taskDatabase.saveAllTasks(dependentTasks)

		for queueName in Set(dependentTasks.map({ return $0.queueName! })) {
			let queue = getQueue(named: queueName)!
			helperDispatchQueue.async {
				queue.startThreads()
			}
		}
	}

	private func removeTasks(_ tasks: [TQTask]) {
		taskDatabase.removeAllTasks(tasks)
	}

	private func setAllRunningTasksToReady() {
		let runningTasks = taskDatabase.getAllRunningTasks()
		for task in runningTasks {
			task.state = .ready
		}
		taskDatabase.saveAllTasks(runningTasks)
	}

	internal func getNextReadyTask(for queue: String) -> TQTask? {
		var nextTask: TQTask? = nil

		synchronized {
			nextTask = taskDatabase.getFirstReadyTask(for: queue)

			if nextTask != nil {
				nextTask!.state = .running
				taskDatabase.saveTask(nextTask!)
			}
		}

		return nextTask
	}

	internal func taskSucceeded(_ task: TQTask) {
		synchronized {
			updateAllTasks(dependingOn: task)
			taskDatabase.removeTask(task)
		}

		helperDispatchQueue.async {
			NotificationCenter.default.post(name: self.taskSucceededNotificationName, object: task)
		}
	}

	internal func taskFailed(_ task: TQTask) -> Bool {
		var rerunNow = false

		synchronized {
			task.retryCounter += 1
			task.totalTryCounter += 1

			if task.totalTryCounter > task.maxNumberOfTries {
				var tasksToRemove = taskDatabase.getAllTasks(dependingOn: task)
				tasksToRemove.append(task)
				removeTasks(tasksToRemove)
			} else if task.retryCounter > task.maxNumberOfRetries {
				task.retryCounter = 0
				moveToEndOfQueue(task)
			} else {
				taskDatabase.saveTask(task)
				rerunNow = true
			}
		}

		helperDispatchQueue.async {
			NotificationCenter.default.post(name: self.taskFailedNotificationName, object: task)
		}

		return rerunNow
	}

	internal func addTask(_ task: TQTask, taskDependancyMap: [TQTaskDependancyMap]? = nil) -> Bool {
		if initialized {
			synchronized {
				let dependencyList = taskDatabase.getDependencyList(for: taskDependancyMap)
				task.dependencyList = dependencyList
				if dependencyList.count > 0 {
					task.state = .notReady
				}
				taskDatabase.addTask(task)
			}
			return true
		} else {
			return false
		}
	}

	public func initialize(key: String, mainBundle bundle: Bundle) -> Bool {
		synchronized {
			mainBundle = bundle
			initialized = taskDatabase.initialize(databaseKey: key)
			if initialized {
				setAllRunningTasksToReady()
				queues = taskDatabase.loadQueues()
			}
		}
		return initialized
	}

	public func uninitialize() {
		synchronized {
			taskDatabase.uninitialize()
			initialized = false
		}
	}

	public func hasQueue(named queueName: String) -> Bool {
		return getQueue(named: queueName) != nil
	}

	public func getQueue(named queueName: String) -> TQQueue? {
		return queues.first(where: { $0.name == queueName })
	}

	public func createQueue(name: String, threadNumber: Int) -> TQQueue? {
		if hasQueue(named: name) {
			return nil
		}

		let queue = TQQueue.init(name: name, threadNumber: threadNumber)
		queues.append(queue)
		taskDatabase.saveQueue(queue)
		return queue
	}

	private override init() {
		super.init()
	}

	public static let shared = TQQueueManager()

}
