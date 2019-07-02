//
//  TQTask.swift
//  AsynchTaskQueue
//
//  Created by joca on 6/29/19.
//  Copyright © 2019 joca. All rights reserved.
//

import Foundation

public enum TQTaskPriority: TimeInterval {
	case veryLow = 1800
	case low = 600
	case normal = 0
	case high = -600
	case veryHigh = -1800
}

public enum TQTaskState {
	case notReady
	case delayed
	case ready
	case running
	case finished
}

open class TQTask {

	public let id: String

	public internal(set) var additionTimestamp: TimeInterval

	public internal(set) var state: TQTaskState

	public internal(set) var retryCounter: Int

	public internal(set) var totalTryCounter: Int

	public var data: [String: Any]
/*
	internal init(id: String, state: TQTaskState = .ready, priority: TQTaskPriority = .normal, retryCounter: Int = 0, totalTryCounter: Int = 0, data: [String: Any]) {
		self.id = UUID.init().uuidString
		self.state = state
		self.retryCounter = retryCounter
		self.totalTryCounter = totalTryCounter
		self.additionTimestamp = Date().timeIntervalSince1970 + priority.rawValue

		self.data = data
	}
*/
	public init(priority: TQTaskPriority = .normal, data: [String: Any]) {
		self.id = UUID.init().uuidString
		self.state = .ready
		self.retryCounter = 0
		self.totalTryCounter = 0
		self.additionTimestamp = Date().timeIntervalSince1970 + priority.rawValue

		self.data = data
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
