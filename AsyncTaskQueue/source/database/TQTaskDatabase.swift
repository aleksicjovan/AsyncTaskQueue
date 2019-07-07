//
//  TQTaskDatabase.swift
//  AsynchTaskQueue
//
//  Created by joca on 6/30/19.
//  Copyright © 2019 joca. All rights reserved.
//

import Foundation
import CouchbaseLiteSwift

internal class TQTaskDatabase {

	private var database: Database!

	private static func getDatabaseNameFromKey(_ key: String) -> String {
		return "TaskDatabase_\(key)"
	}

	private func getTaskQuery(expression: ExpressionProtocol?, queue: String? = nil, limit: Int = Int.max) -> Query {
		var fullExpression = Expression.property("type").equalTo(Expression.string("task"))
		if let queue = queue {
			let queueExpression = Expression.property("queue").equalTo(Expression.string(queue))
			fullExpression = fullExpression.and(queueExpression)
		}
		if expression != nil {
			fullExpression = fullExpression.and(expression!)
		}

		let query: Query = QueryBuilder
			.select(SelectResult.all())
			.from(DataSource.database(database))
			.where(fullExpression)
			.orderBy(Ordering.property("additionTimestamp").ascending())
			.limit(Expression.int(limit))

		return query
	}

	private func getDependancyExpression(_ dependancy: TQTaskDependancyMap) -> ExpressionProtocol? {
		var typeExpression: ExpressionProtocol?
		if let type = dependancy.type {
			let taskType = NSStringFromClass(type)
			typeExpression = Expression.property("taskType").equalTo(Expression.string(taskType))
		}

		var referenceExpression: ExpressionProtocol?
		if let dependancyReferenceList = dependancy.dependancyReferenceList {
			for reference in dependancyReferenceList {
				let oneReferenceExpression = ArrayFunction.contains(Expression.property("referenceIds"), value: Expression.string(reference))
				if referenceExpression != nil {
					referenceExpression = referenceExpression!.and(oneReferenceExpression)
				} else {
					referenceExpression = oneReferenceExpression
				}
			}
		}

		var dependencyExpression: ExpressionProtocol?
		if typeExpression != nil && referenceExpression != nil {
			dependencyExpression = typeExpression!.and(referenceExpression!)
		} else {
			dependencyExpression = typeExpression ?? dependencyExpression
		}
		return dependencyExpression
	}

	internal func getDependencyList(for dependancyMap: [TQTaskDependancyMap]?) -> [String] {
		guard let dependancyMap = dependancyMap else {
			return []
		}

		var expression: ExpressionProtocol?
		for dependancy in dependancyMap {
			let dependencyExpression = getDependancyExpression(dependancy)
			if expression != nil && dependencyExpression != nil {
				expression = expression!.or(dependencyExpression!)
			} else {
				expression = expression ?? dependencyExpression
			}
		}

		guard let whereExpression = expression else {
			return []
		}

		let query = QueryBuilder
			.select(SelectResult.expression(Meta.id))
			.from(DataSource.database(database))
			.where(whereExpression)
		let results = try! query.execute()

		var dependancyList = [String]()
		for result in results {
			dependancyList.append(result.string(forKey: "id")!)
		}

		return dependancyList
	}

	internal func getFirstReadyTask(for queue: String) -> TQTask? {
		let expression = Expression.property("state").equalTo(Expression.string(TQTaskState.ready.rawValue))
		let query = getTaskQuery(expression: expression, queue: queue, limit: 1)

		let resultSet = try! query.execute()
		if let result = resultSet.next() {
			return TQTaskDatabase.deserializeTask(result.dictionary(forKey: database.name)!.toDictionary())
		} else {
			return nil
		}
	}

	internal func getAllTasks(dependingOn task: TQTask) -> [TQTask] {
		let expression = ArrayFunction.contains(Expression.property("dependencyList"), value: Expression.string(task.id))
		let query = getTaskQuery(expression: expression)

		var dependentTasks = [TQTask]()
		for result in try! query.execute() {
			let taskDictionary = result.dictionary(forKey: database.name)!.toDictionary()
			let dependentTask = TQTaskDatabase.deserializeTask(taskDictionary)
			dependentTasks.append(dependentTask)
		}

		return dependentTasks
	}

	internal func getAllRunningTasks() -> [TQTask] {
		let expression = Expression.property("state").equalTo(Expression.string(TQTaskState.running.rawValue))
		let query = getTaskQuery(expression: expression)

		var runningTasks = [TQTask]()
		for result in try! query.execute() {
			let taskDictionary = result.dictionary(forKey: database.name)!.toDictionary()
			let task = TQTaskDatabase.deserializeTask(taskDictionary)
			runningTasks.append(task)
		}

		return runningTasks
	}

	internal func addTask(_ task: TQTask) {
		task.createdAt = Date().timeIntervalSince1970
		saveTask(task)
	}

	internal func saveTask(_ task: TQTask) {
		task.updatedAt = Date().timeIntervalSince1970
		let doc = TQTaskDatabase.serializeTask(task)
		doc.setString("task", forKey: "type")
		try! database.saveDocument(doc)
	}

	internal func saveAllTasks(_ tasks: [TQTask]) {
		try! database.inBatch {
			for task in tasks {
				saveTask(task)
			}
		}
	}

	internal func removeTask(_ task: TQTask) {
		let doc = database.document(withID: task.id)!
		try! database.deleteDocument(doc)
	}

	internal func removeAllTasks(_ tasks: [TQTask]) {
		try! database.inBatch {
			for task in tasks {
				removeTask(task)
			}
		}
	}

	internal func loadQueues() -> [TQQueue] {
		let query = QueryBuilder
			.select(SelectResult.all())
			.from(DataSource.database(database))
			.where(Expression.property("type").equalTo(Expression.string("queue")))

		var queues = [TQQueue]()

		for result in try! query.execute() {
			let queueDictionary = result.dictionary(forKey: database.name)!.toDictionary()
			let queue = TQTaskDatabase.deserializeQueue(queueDictionary)

			queues.append(queue)
		}

		return queues
	}

	internal func saveQueue(_ queue: TQQueue) {
		let doc = TQTaskDatabase.serializeQueue(queue)
		doc.setString("queue", forKey: "type")
		try! database.saveDocument(doc)
	}

	internal func initialize(databaseKey: String) -> Bool {
		do {
			let databaseName = TQTaskDatabase.getDatabaseNameFromKey(databaseKey)
			database = try Database(name: databaseName)
			return true
		} catch {
			return false
		}
	}

	internal func uninitialize() {
		database = nil
	}

}

extension TQTaskDatabase {

	private static func serializeTask(_ task: TQTask) -> MutableDocument {
		let doc = MutableDocument(id: task.id)

		let taskClass = type(of: task)
		let taskType = NSStringFromClass(taskClass)
		doc.setString(taskType, forKey: "taskType")
		doc.setString(task.id, forKey: "id")
		doc.setDouble(task.createdAt, forKey: "createdAt")
		doc.setDouble(task.updatedAt, forKey: "updatedAt")

		if let name = task.name {
			doc.setString(name, forKey: "name")
		}
		doc.setDouble(task.additionTimestamp, forKey: "additionTimestamp")
		doc.setString(task.state.rawValue, forKey: "state")
		doc.setInt(task.retryCounter, forKey: "retryCounter")
		doc.setInt(task.totalTryCounter, forKey: "totalTryCounter")
		if let queue = task.queueName {
			doc.setString(queue, forKey: "queue")
		}

		doc.setValue(task.dependencyList, forKey: "dependencyList")
		doc.setValue(task.referenceIds, forKey: "referenceIds")

		doc.setValue(task.data, forKey: "data")

		return doc
	}

	private static func deserializeTask(_ doc: [String: Any]) -> TQTask {
		let taskType = doc["taskType"] as! String
		let anyClass: AnyClass? = TQQueueManager.shared.mainBundle.classNamed(taskType)
		let taskClass = anyClass as! TQTask.Type

		let data = doc["data"] as! [String: Any]
		let dependencyList = doc["dependencyList"] as! [String]
		let referenceIds = doc["referenceIds"] as! [String]
		let task = taskClass.init(data: data, referenceIds: referenceIds, dependencyList: dependencyList)

		task.id = doc["id"] as! String
		task.createdAt = doc["createdAt"] as! Double
		task.updatedAt = doc["updatedAt"] as! Double

		task.name = doc["name"] as? String
		task.additionTimestamp = doc["additionTimestamp"] as! Double
		task.state = TQTaskState(rawValue: (doc["state"] as! String))!
		task.retryCounter = doc["retryCounter"] as! Int
		task.totalTryCounter = doc["totalTryCounter"] as! Int
		task.queueName = doc["queue"] as? String

		return task
	}

}

extension TQTaskDatabase {

	private static func serializeQueue(_ queue: TQQueue) -> MutableDocument {
		let doc = MutableDocument()

		doc.setString(queue.name, forKey: "name")
		doc.setInt(queue.threadNumber, forKey: "threadNumber")

		return doc
	}

	private static func deserializeQueue(_ doc: [String: Any]) -> TQQueue {
		let name = doc["name"] as! String
		let threadNumber = doc["threadNumber"] as! Int

		let queue = TQQueue(name: name, threadNumber: threadNumber)

		return queue
	}

}
