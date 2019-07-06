//
//  TQMonitor.swift
//  AsyncTaskQueue
//
//  Created by joca on 7/6/19.
//  Copyright © 2019 joca. All rights reserved.
//

import Foundation

public class TQMonitor {

	private var currentThread: Thread?

	private let synchronizationSemaphore = DispatchSemaphore(value: 1)

	public func synchronized(_ block: () -> Void) {
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

}
