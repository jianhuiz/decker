require 'tempfile'
require 'tmpdir'
require 'yaml'
require 'shellwords'

require 'container/container'

require 'dea/utils/download'
require 'dea/utils/upload'
require 'dea/promise'
require 'dea/task'
require 'dea/env'
require 'dea/staging/admin_buildpack_downloader'
require 'dea/staging/staging_task_workspace'
require 'dea/staging/staging_message'
require 'dea/loggregator'

module Dea
  class StagingTask < Task
    class StagingError < StandardError
      def initialize(msg)
        super("Error staging: #{msg}")
      end
    end

    class StagingTaskStoppedError < StagingError
      def initialize
        super('task stopped')
      end
    end

    attr_reader :bootstrap, :dir_server, :staging_message, :task_id, :droplet_sha1

    def initialize(bootstrap, dir_server, staging_message, buildpacks_in_use, custom_logger=nil)
      super(bootstrap.config, custom_logger)
      @bootstrap = bootstrap
      @dir_server = dir_server
      @staging_message = staging_message
      @task_id = staging_message.task_id
      @buildpacks_in_use = buildpacks_in_use

      logger.user_data[:task_id] = task_id
    end

    def start
      staging_promise = Promise.new do |p|
        resolve_staging_setup
        resolve_staging
        p.deliver
      end

      Promise.resolve(staging_promise) do |error, _|
        begin
          if error
            logger.info 'staging.task.failed',
              error: error, backtrace: error.backtrace
          else
            logger.info 'staging.task.completed'
          end

          unless error
            begin
              resolve_staging_upload
            rescue => e
              logger.info 'staging.task.upload-failed',
                error: e,
                backtrace: e.backtrace

              error = e
            end
          end

          trigger_after_complete(error)

          raise(error) if error
        ensure
          FileUtils.rm_rf(workspace.workspace_dir)
        end
      end
    end

    def workspace
      @workspace ||= StagingTaskWorkspace.new(
        config['base_dir'],
        staging_message,
        @buildpacks_in_use
      )
    end

    def task_log
      File.read(workspace.staging_log_path) if File.exists?(workspace.staging_log_path)
    end

    def streaming_log_url
      @dir_server.staging_task_file_url_for(task_id, workspace.warden_staging_log)
    end

    def task_info
      File.exists?(workspace.staging_info_path) ? YAML.load_file(workspace.staging_info_path) : {}
    end

    def detected_buildpack
      "Docker"
    end

    def memory_limit_mb
      [(config.minimum_staging_memory_mb).to_i, staging_message.start_message.mem_limit.to_i].max
    end

    def memory_limit_in_bytes
      memory_limit_mb * 1024 * 1024
    end
    alias :used_memory_in_bytes :memory_limit_in_bytes

    def disk_limit_mb
      [(config.minimum_staging_disk_mb).to_i, staging_message.start_message.disk_limit.to_i].max
    end

    def disk_limit_in_bytes
      disk_limit_mb * 1024 * 1024
    end

    def disk_inode_limit
      config.staging_disk_inode_limit
    end

    def stop(&callback)
      stopping_promise = Promise.new do |p|
        logger.info 'staging.task.stopped'

        @after_complete_callback = nil # Unregister after complete callback
        promise_stop.resolve if container.handle
        p.deliver
      end

      Promise.resolve(stopping_promise) do |error, _|
        trigger_after_stop(StagingTaskStoppedError.new)
        callback.call(error) unless callback.nil?
      end
    end

    def after_setup_callback(&blk)
      @after_setup_callback = blk
    end

    def trigger_after_setup(error)
      @after_setup_callback.call(error) if @after_setup_callback
    end
    private :trigger_after_setup

    def after_complete_callback(&blk)
      @after_complete_callback = blk
    end

    def trigger_after_complete(error)
      @after_complete_callback.call(error) if @after_complete_callback
    end
    private :trigger_after_complete

    def after_stop_callback(&blk)
      @after_stop_callback = blk
    end

    def trigger_after_stop(error)
      @after_stop_callback.call(error) if @after_stop_callback
    end

    private :trigger_after_stop

    def promise_prepare_staging_log
      Promise.new do |p|
        script = "mkdir -p #{workspace.warden_staged_dir}/logs && touch #{workspace.warden_staging_log}"

        logger.info 'staging.task.preparing-log', script: script

        loggregator_emit_result `#{script}`

        p.deliver
      end
    end

    def promise_app_dir
      Promise.new do |p|
        # Some buildpacks seem to make assumption that /app is a non-empty directory
        # See: https://github.com/heroku/heroku-buildpack-python/blob/master/bin/compile#L46
        script = "mkdir -p #{workspace.warden_staged_dir}/app"

        logger.info 'staging.task.making-app-dir', script: script

        loggregator_emit_result `#{script}`

        p.deliver
      end
    end

    def promise_unpack_app
      Promise.new do |p|
        logger.info 'staging.task.unpacking-app', destination: workspace.warden_unstaged_dir

        script = "mkdir -p #{workspace.warden_unstaged_dir} && unzip -q #{workspace.downloaded_app_package_path} -d #{workspace.warden_unstaged_dir}"
        logger.info 'staging.task.unpack-app', script: script
        loggregator_emit_result `#{script}`

        p.deliver
      end
    end

    def promise_pack_app
      Promise.new do |p|
        logger.info 'staging.task.packing-droplet'

        script = "mkdir -p #{File.dirname(workspace.staged_droplet_path)} && cd #{workspace.warden_unstaged_dir} && COPYFILE_DISABLE=true tar -czf #{workspace.staged_droplet_path} ."
        logger.info 'staging.task.pack-app', script: script
        p.deliver
      end
    end

    def promise_app_download
      Promise.new do |p|
        logger.info 'staging.app-download.starting', uri: staging_message.download_uri

        download_destination = Tempfile.new('app-package-download.tgz')

        Download.new(staging_message.download_uri, download_destination, nil, logger).download! do |error|
          if error
            logger.debug 'staging.app-download.failed',
              duration: p.elapsed_time,
              error: error,
              backtrace: error.backtrace

            p.fail(error)
          else
            File.rename(download_destination.path, workspace.downloaded_app_package_path)
            File.chmod(0744, workspace.downloaded_app_package_path)

            logger.debug 'staging.app-download.completed',
              duration: p.elapsed_time,
              destination: workspace.downloaded_app_package_path

            p.deliver
          end
        end
      end
    end

    def promise_log_upload_started
      Promise.new do |p|
        log_to_staging_log("-----> Uploading droplet ($droplet_size)")
        p.deliver
      end
    end

    def promise_app_upload
      Promise.new do |p|
        logger.info 'staging.droplet-upload.starting',
          source: workspace.staged_droplet_path,
          destination: staging_message.upload_uri

        Upload.new(workspace.staged_droplet_path, staging_message.upload_uri, logger).upload! do |error|
          if error
            logger.info 'staging.task.droplet-upload-failed',
              duration: p.elapsed_time,
              destination: staging_message.upload_uri,
              error: error,
              backtrace: error.backtrace

            p.fail(error)
          else
            logger.info 'staging.task.droplet-upload-completed',
              duration: p.elapsed_time,
              destination: staging_message.upload_uri

            p.deliver
          end
        end
      end
    end

    def promise_save_droplet
      Promise.new do |p|
        @droplet_sha1 = Digest::SHA1.file(workspace.staged_droplet_path).hexdigest
        bootstrap.droplet_registry[@droplet_sha1].local_copy(workspace.staged_droplet_path) do |error|
          if error
            logger.error 'staging.droplet.copy-failed',
              error: error, backtrace: error.backtrace

            p.fail
          else
            p.deliver
          end
        end
      end
    end

    def path_in_container(path)
      File.join(container.path, 'tmp', 'rootfs', path.to_s) if container.path
    end

    def staging_config
      config['staging']
    end

    def staging_timeout
      (staging_config['max_staging_duration'] || '900').to_f
    end

    def bind_mounts
      [workspace.workspace_dir, workspace.buildpack_dir, workspace.admin_buildpacks_dir].collect do |path|
        {'src_path' => path, 'dst_path' => path}
      end + config['bind_mounts']
    end

    def snapshot_attributes
      logger.info 'snapshot_attributes', properties: staging_message.properties
      {
        'staging_message' => staging_message.to_hash,
        'warden_container_path' => container.path,
        'warden_job_id' => @warden_job_id,
        'syslog_drain_urls' => syslog_drain_urls,
      }
    end

    private

    def staging_command
      env = Env.new(staging_message, self)

      [
        'set -o pipefail;',
        env.exported_environment_variables,
        config['dea_ruby'],
        run_plugin_path,
        workspace.plugin_config_path,
        "| tee -a #{workspace.warden_staging_log}"
      ].join(' ')
    end

    def syslog_drain_urls
      services = staging_message.properties['services'] || []
      services.map { |svc_hash| svc_hash['syslog_drain_url'] }.compact
    end

    def resolve_staging_setup
      workspace.prepare

      promise_app_download.resolve
      promise_prepare_staging_log.resolve

    rescue => e
      trigger_after_setup(e)
      raise
    else
      trigger_after_setup(nil)
    end

    def resolve_staging
      Promise.run_serially(
        promise_unpack_app,
        promise_pack_app,
        promise_save_droplet,
        promise_log_upload_started
      )
    end

    def resolve_staging_upload
      promise_app_upload.resolve
    end

    def run_plugin_path
      File.join(workspace.buildpack_dir, 'bin/run')
    end

    def staging_timeout_grace_period
      60
    end

    def loggregator_emit_result(result)
      if (result != nil)
        Dea::Loggregator.staging_emit(staging_message.app_id, result.stdout)
        Dea::Loggregator.staging_emit_error(staging_message.app_id, result.stderr)
      end
      result
    end

    def log_to_staging_log(message)
      File.open(workspace.warden_staging_log, "a+") { |f| f.write(message) }
    end
  end
end
