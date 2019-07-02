//
//  TQTaskDatabase.swift
//  AsynchTaskQueue
//
//  Created by joca on 6/30/19.
//  Copyright © 2019 joca. All rights reserved.
//

import Foundation
import CouchbaseLiteSwift

class TQTaskDatabase {

	private var taskList = [TQTask]()

	public func getFirstReadyTask(requiresInternetConnection: Bool = false) -> TQTask? {
		let sortedTasks = taskList.sorted { (first, second) -> Bool in
			return first.additionTimestamp < second.additionTimestamp
		}
		let task = sortedTasks.first(where: { task in
			return task.state == .ready
		})
		return task
	}

	public func saveTask(_ task: TQTask) {
		removeTask(task)
		taskList.insert(task, at: 0)
	}

	public func addTask(_ task: TQTask) {
		taskList.append(task)
	}

	public func removeTask(_ task: TQTask) {
		taskList.removeAll(where: { queuedTask in
			queuedTask.id == task.id
		})
	}

	public func moveToEndOfQueue(_ task: TQTask) {
		task.additionTimestamp = Date().timeIntervalSince1970
	}

}
