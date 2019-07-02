
Pod::Spec.new do |s|

  s.name         = "AsyncTaskQueue"
  s.version      = "0.0.1"
  s.summary      = "A short description of AsyncTaskQueue."

  s.description  = <<-DESC
					Something!!!
                   DESC

  s.homepage     = "http://EXAMPLE/AsyncTaskQueue"
  # s.license      = { :type => "MIT", :file => "FILE_LICENSE" }

  s.author             = { "aleksicjovan" => "jovan.aleksic.a@gmail.com" }

  s.ios.deployment_target = "12.0"

  s.source       = { :path => "./" }

  s.source_files  = "Classes", "**/*.{h,m,swift}"
  s.exclude_files = "Classes/Exclude"

  # s.public_header_files = "Classes/**/*.h"

  # s.xcconfig = { "HEADER_SEARCH_PATHS" => "$(SDKROOT)/usr/include/libxml2" }
  s.dependency "CouchbaseLite-Swift", "~> 2.5.1"

end
