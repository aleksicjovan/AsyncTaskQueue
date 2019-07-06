//
//  TQTask.swift
//  AsynchTaskQueue
//
//  Created by joca on 6/29/19.
//  Copyright © 2019 joca. All rights reserved.
//

import Foundation
import CouchbaseLiteSwift

public enum TQTaskPriority: TimeInterval {
	case veryLow = 1800
	case low = 600
	case normal = 0
	case high = -600
	case veryHigh = -1800
}

public enum TQTaskState: String {
	case notReady
	case ready
	case running
	case finished
}

open class TQTask {

	public internal(set) var id = UUID.init().uuidString

	public internal(set) var additionTimestamp = Date().timeIntervalSince1970

	public internal(set) var state = TQTaskState.ready

	public internal(set) var retryCounter = 0

	public internal(set) var totalTryCounter = 0

	public internal(set) var referenceIds = [String]()

	public internal(set) var dependencyList = [String]()

	public var data: [String: Any]

	required public init(data: [String: Any], referenceIds: [String], dependencyList: [String]) {
		self.data = data
		self.referenceIds = referenceIds
		self.dependencyList = dependencyList
	}

	public init(data: [String: Any], priority: TQTaskPriority = .normal, referenceIds: [String] = []) {
		self.data = data
		self.additionTimestamp += priority.rawValue
		self.referenceIds = referenceIds
	}

	internal final func run() {
		var runningError: Error? = nil

		let semaphore = DispatchSemaphore(value: 0)
		do {
			print("Running task \(id)")
			try execute { error in
				print("Task finished with error \(error?.localizedDescription ?? "'none'")")
				runningError = error
				semaphore.signal()
			}
		} catch {
			print("Task failed with error \(error.localizedDescription)")
			runningError = error
			semaphore.signal()
		}
		semaphore.wait()

		let rerun = TQQueue.shared.taskFinished(self, error: runningError)
		if rerun {
			print("Rerunning task \(id)")
			run()
		} else {
			print("Not rerunning task \(id)")
		}
	}

	open func execute(_ onCompletion: (Error?) -> Void) throws {
		preconditionFailure("This method must be overriden in all classes subclassed from TQTask")
	}

	open var maxNumberOfRetries: Int {
		return TQConfig.MAX_NUMBER_OF_RETRIES
	}

	open var maxNumberOfTries: Int {
		return TQConfig.MAX_NUMBER_OF_TRIES
	}

}

extension TQTask {

	internal static func serializeTask(_ task: TQTask) -> MutableDocument {
		let doc = MutableDocument(id: task.id)

		let taskClass = type(of: task)
		let taskType = NSStringFromClass(taskClass)
		doc.setString(taskType, forKey: "taskType")
		doc.setString(task.id, forKey: "id")

		doc.setDouble(task.additionTimestamp, forKey: "additionTimestamp")
		doc.setString(task.state.rawValue, forKey: "state")
		doc.setInt(task.retryCounter, forKey: "retryCounter")
		doc.setInt(task.totalTryCounter, forKey: "totalTryCounter")

		doc.setValue(task.dependencyList, forKey: "dependencyList")
		doc.setValue(task.referenceIds, forKey: "referenceIds")

		doc.setValue(task.data, forKey: "data")

		return doc
	}

	internal static func deserializeTask(_ doc: [String: Any]) -> TQTask {
		let taskType = doc["taskType"] as! String
		let anyClass: AnyClass? = TQQueue.shared.mainBundle.classNamed(taskType)
		let taskClass = anyClass as! TQTask.Type

		let data = doc["data"] as! [String: Any]
		let dependencyList = doc["dependencyList"] as! [String]
		let referenceIds = doc["referenceIds"] as! [String]
		let task = taskClass.init(data: data, referenceIds: referenceIds, dependencyList: dependencyList)

		task.id = doc["id"] as! String
		task.additionTimestamp = doc["additionTimestamp"] as! Double
		task.state = TQTaskState(rawValue: (doc["state"] as! String))!
		task.retryCounter = doc["retryCounter"] as! Int
		task.totalTryCounter = doc["totalTryCounter"] as! Int

		return task
	}

}
