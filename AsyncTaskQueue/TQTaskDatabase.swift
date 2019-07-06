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

	internal func getFirstReadyTask(for queue: String) -> TQTask? {
		let expression = Expression.property("state").equalTo(Expression.string(TQTaskState.ready.rawValue))
		let query = getTaskQuery(expression: expression, queue: queue, limit: 1)

		let resultSet = try! query.execute()
		if let result = resultSet.next() {
			return TQTask.deserializeTask(result.dictionary(forKey: database.name)!.toDictionary())
		} else {
			return nil
		}
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

	internal func updateAllDependentTasks(_ task: TQTask) {
		let expression = ArrayFunction.contains(Expression.property("dependencyList"), value: Expression.string(task.id))
		let query = getTaskQuery(expression: expression)

		var dependentTasks = [TQTask]()
		for result in try! query.execute() {
			let taskDictionary = result.dictionary(forKey: database.name)!.toDictionary()
			let dependentTask = TQTask.deserializeTask(taskDictionary)

			dependentTask.dependencyList.removeAll(where: { $0 == task.id })
			if dependentTask.dependencyList.count == 0 {
				dependentTask.state = .ready
			}

			dependentTasks.append(dependentTask)
		}

		try! database.inBatch {
			for dependentTask in dependentTasks {
				saveTask(dependentTask)
			}
		}
	}

	internal func saveTask(_ task: TQTask) {
		let doc = TQTask.serializeTask(task)
		doc.setString("task", forKey: "type")
		try! database.saveDocument(doc)
	}

	internal func addTask(_ task: TQTask) {
		saveTask(task)
	}

	internal func removeTask(_ task: TQTask) {
		if let doc = database.document(withID: task.id) {
			do {
				try database.deleteDocument(doc)
			} catch {
				print("Database errors: error deleting document: \(error)")
			}
		} else {
			print("Database errors: no task in database")
		}
	}

	private func getDatabaseNameFromKey(_ key: String) -> String {
		return "Database-\(key)"
	}

	internal func initialize(databaseKey: String) -> Bool {
		do {
			let databaseName = getDatabaseNameFromKey(databaseKey)
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
