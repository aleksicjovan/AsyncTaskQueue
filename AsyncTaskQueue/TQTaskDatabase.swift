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

	private var database: Database!

	internal func getFirstReadyTask(requiresInternetConnection: Bool = false) -> TQTask? {
		let query = QueryBuilder
			.select(SelectResult.all())
			.from(DataSource.database(database))
			.where(Expression.property("state").equalTo(Expression.string(TQTaskState.ready.rawValue)))
			.orderBy(Ordering.property("additionTimestamp").ascending())
			.limit(Expression.int(1))

		let resultSet = try! query.execute()
		if let result = resultSet.next() {
			return TQTask.deserializeTask(result.dictionary(forKey: database.name)!.toDictionary())
		} else {
			return nil
		}
	}

	internal func saveTask(_ task: TQTask) {
		let doc = TQTask.serializeTask(task)
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
