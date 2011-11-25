require 'fileutils'
require 'clamp'
class Delayed::WorkerCommand < Clamp::Command
  subcommand "start", "Start a worker" do   
    parameter "QUEUE", "the queue from which to take jobs", :default => "delayed_job"
    option "--min-priority", "MIN_PRIORITY", "minimum priority of jobs to handle", :default => nil
    option "--max-priority", "MAX_PRIORITY", "maximum priority of jobs to handle", :default => nil

    def execute
      Delayed::Worker.logger = Logger.new(log_file_name(queue))
      File.open(pid_file_name(queue), "w+"){|f| f << Process.pid}        
      Delayed::Worker.logger.info "Wrote pid to #{pid_file_name(queue)}"

      Delayed::Worker.new(
      :min_priority => min_priority, 
      :max_priority => max_priority, 
      :quiet => false,
      :queues => [queue]
      ).start
    end  
  end

  subcommand "stop", "Stop a worker" do   
    parameter "QUEUE", "the queue from which to take jobs", :default => "delayed_job"  
    def execute
      Delayed::Worker.logger = Logger.new(log_file_name(queue))      
      begin
        pid = File.read(pid_file_name(queue)).to_i
        Delayed::Worker.logger.info "Killing #{pid}..."

        Timeout::timeout(20) do 
          Process.kill("SIGABRT", pid) 
          Process.wait(pid)
        end
      rescue Errno::ESRCH
        Delayed::Worker.logger.info "No such process, or it's already dead"
      rescue Timeout::Error
        Delayed::Worker.logger.info "Timeout, forcing kill"
        Process.kill("SIGKILL", pid)
      rescue Errno::ENOENT
        Delayed::Worker.logger.info "Could not open pid file #{pid_file_name(queue)}"
      end
      Delayed::Worker.logger.info "Removing pid file #{pid_file_name(queue)}"
      FileUtils.rm_f(pid_file_name(queue))
    end    
  end

  def pid_dir
    File.join(Rails.root, "tmp", "pids")
  end

  def log_dir
    File.join(Rails.root, "log")
  end

  def log_file_name(queue)
    FileUtils.mkdir_p(log_dir)
    File.join(log_dir, "delayed_job_#{queue}.log")    
  end

  def pid_file_name(queue)      
    FileUtils.mkdir_p(pid_dir)
    File.join(pid_dir, "delayed_job_#{queue}.pid")
  end
end