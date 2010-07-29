FOLDER = "~/.heroku/plugins/heroku-postgresql"

desc "Install via copy"
task :install do
  sh "rm -rf #{FOLDER}; cp -R . #{FOLDER}"
end

desc "Install via symlink"
task :install_dev do
  sh "rm -rf #{FOLDER}; ln -s #{File.dirname(__FILE__)} #{FOLDER}"
end
