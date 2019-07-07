//
//  TQThread.swift
//  AsynchTaskQueue
//
//  Created by joca on 6/30/19.
//  Copyright © 2019 joca. All rights reserved.
//

import Foundation

internal class TQThread: Thread {

	let queue: TQQueue

	internal init(queue: TQQueue) {
		self.queue = queue
	}

	internal override func main() {
		while true {
			var shouldBreak = false

			autoreleasepool {
				if let task = queue.getNextReadyTask() {
					runTask(task)
				} else {
					shouldBreak = true
				}
			}

			if shouldBreak {
				break
			}
		}
	}

	private func runTask(_ task: TQTask) {
		let error = task.run()
		if error != nil {
			let rerun = queue.taskFailed(task, error: error!)
			if rerun {
				runTask(task)
			}
		} else {
			queue.taskSucceeded(task)
		}
	}
}
