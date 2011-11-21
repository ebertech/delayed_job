require 'fileutils'
require 'clamp'
class Delayed::WorkerCommand < Clamp::Command
  subcommand "start", "Start a worker" do   
    parameter "QUEUE", "the queue from which to take jobs", :default => "delayed_job"
    option "--min-priority", "MIN_PRIORITY", "minimum priority of jobs to handle", :default => nil
    option "--max-priority", "MAX_PRIORITY", "maximum priority of jobs to handle", :default => nil

    def execute
      File.open(pid_file_name(queue), "w+"){|f| f << Process.pid}        
      puts "Wrote pid to #{pid_file_name(queue)}"
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
      pid = File.read(pid_file_name(queue)).to_i
      puts "Killing #{pid}..."
      begin
        Timeout::timeout(20) do 
          Process.kill("SIGABRT", pid) 
          Process.wait(pid)
        end
      rescue Errno::ESRCH
        puts "No such process, or it's already dead"
      rescue Timeout::Error
        puts "Timeout, forcing kill"
        Process.kill("SIGKILL", pid)
      end
      puts "Removing pid file #{pid_file_name(queue)}"
      FileUtils.rm(pid_file_name(queue))
    end    
  end

  def pid_dir
    File.join(Rails.root, "tmp", "pids")
  end

  def pid_file_name(queue)      
    FileUtils.mkdir_p(pid_dir)
    File.join(pid_dir, "#{queue}.pid")
  end
end