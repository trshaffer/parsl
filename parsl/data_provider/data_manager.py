import os
import logging
import concurrent.futures as cf
from parsl.data_provider.scheme import GlobusScheme
from parsl.executors.base import ParslExecutor
from parsl.data_provider.ftp import _ftp_stage_in_app
from parsl.data_provider.globus import get_globus
from parsl.data_provider.http import _http_stage_in_app
from parsl.app.app import python_app

from typing import cast, List

logger = logging.getLogger(__name__)

from typing import TYPE_CHECKING
from typing import Optional

if TYPE_CHECKING:
    from parsl.app.futures import DataFuture
    from parsl.data_provider.files import File # for mypy
    from parsl.data_provider.files import File
    from parsl.dataflow.dflow import DataFlowKernel

# BUG? both _http_stage_in and _ftp_stage_in take a list of files,
# but only ever dosomething with the first elemtn of that list - 
# not handling the multiple element (or 0 element) case

class DataManager(ParslExecutor):
    """The DataManager is responsible for transferring input and output data.

    It uses the Executor interface, where staging tasks are submitted
    to it, and DataFutures are returned.
    """

    def __init__(self, dfk: "DataFlowKernel", max_threads=10) -> None:
        """Initialize the DataManager.

        Args:
           - dfk (DataFlowKernel): The DataFlowKernel that this DataManager is managing data for.

        Kwargs:
           - max_threads (int): Number of threads. Default is 10.
           - executors (list of Executors): Executors for which data transfer will be managed.
        """
        self._scaling_enabled = False

        self.label = 'data_manager'
        self.dfk = dfk
        self.max_threads = max_threads
        self.globus = None
        self.managed = True

    def start(self) -> None:
        self.executor = cf.ThreadPoolExecutor(max_workers=self.max_threads)

    def submit(self, *args, **kwargs):
        """Submit a staging app. All optimization should be here."""
        return self.executor.submit(*args, **kwargs)

    def scale_in(self, blocks) -> None:
        pass

    def scale_out(self, *args, **kwargs):
        pass

    def shutdown(self, block=False):
        """Shutdown the ThreadPool.

        Kwargs:
            - block (bool): To block for confirmations or not

        """
        x = self.executor.shutdown(wait=block)
        logger.debug("Done with executor shutdown")
        return x

    @property
    def scaling_enabled(self):
        return self._scaling_enabled

    def initialize_globus(self):
        if self.globus is None:
            self.globus = get_globus()

    def _get_globus_endpoint(self, executor_label: Optional[str] = None):
        if executor_label is None:
            raise ValueError("executor_label is mandatory")
        executor = self.dfk.executors[executor_label]
        if not hasattr(executor, "storage_access"):
            raise ValueError("specified executor does not have storage_access attribute")
        for scheme in executor.storage_access:
            if isinstance(scheme, GlobusScheme):
                if executor.working_dir:
                    working_dir = os.path.normpath(executor.working_dir)
                else:
                    raise ValueError("executor working_dir must be specified for GlobusScheme")
                if scheme.endpoint_path and scheme.local_path:
                    endpoint_path = os.path.normpath(scheme.endpoint_path)
                    local_path = os.path.normpath(scheme.local_path)
                    common_path = os.path.commonpath((local_path, working_dir))
                    if local_path != common_path:
                        raise Exception('"local_path" must be equal or an absolute subpath of "working_dir"')
                    relative_path = os.path.relpath(working_dir, common_path)
                    endpoint_path = os.path.join(endpoint_path, relative_path)
                else:
                    endpoint_path = working_dir
                return {'endpoint_uuid': scheme.endpoint_uuid,
                        'endpoint_path': endpoint_path,
                        'working_dir': working_dir}
        raise Exception('No suitable Globus endpoint defined for executor {}'.format(executor_label))

    def stage_in(self, file: "File", executor: str) -> "DataFuture":
        """Transport the file from the input source to the executor.

        This function returns a DataFuture.

        Args:
            - self
            - file (File) : file to stage in
            - executor (str) : an executor the file is going to be staged in to.
        """

        if file.scheme == 'ftp':
            working_dir = self.dfk.executors[executor].working_dir
            stage_in_app = _ftp_stage_in_app(self, executor=executor)
            app_fut = stage_in_app(working_dir, outputs=[file])
            return app_fut._outputs[0]
        elif file.scheme == 'http' or file.scheme == 'https':
            working_dir = self.dfk.executors[executor].working_dir
            stage_in_app = _http_stage_in_app(self, executor=executor)
            app_fut = stage_in_app(working_dir, outputs=[file])
            return app_fut._outputs[0]
        elif file.scheme == 'globus':
            globus_ep = self._get_globus_endpoint(executor)
            stage_in_app = self._globus_stage_in_app()
            app_fut = stage_in_app(globus_ep, outputs=[file])
            return app_fut._outputs[0]
        else:
            raise Exception('Staging in with unknown file scheme {} is not supported'.format(file.scheme))

    def _globus_stage_in_app(self):
        return python_app(executors=['data_manager'], data_flow_kernel=self.dfk)(self._globus_stage_in)

    def _globus_stage_in(self, globus_ep, outputs=[]):
        file = outputs[0]
        file.local_path = os.path.join(
                globus_ep['working_dir'], file.filename)
        dst_path = os.path.join(
                globus_ep['endpoint_path'], file.filename)

        self.initialize_globus()

        self.globus.transfer_file(
                file.netloc, globus_ep['endpoint_uuid'],
                file.path, dst_path)

    def stage_out(self, file: "File", executor: Optional[str]) -> "DataFuture":
        """Transport the file from the local filesystem to the remote Globus endpoint.

        This function returns a DataFuture.

        Args:
            - self
            - file (File) - file to stage out
            - executor (str) - Which executor the file is going to be staged out from.
        """

        if file.scheme == 'http' or file.scheme == 'https':
            raise Exception('HTTP/HTTPS file staging out is not supported')
        elif file.scheme == 'ftp':
            raise Exception('FTP file staging out is not supported')
        elif file.scheme == 'globus':
            globus_ep = self._get_globus_endpoint(executor)
            stage_out_app = self._globus_stage_out_app()
            return stage_out_app(globus_ep, inputs=[file])
        else:
            raise Exception('Staging out with unknown file scheme {} is not supported'.format(file.scheme))

    def _globus_stage_out_app(self):
        return python_app(executors=['data_manager'], data_flow_kernel=self.dfk)(self._globus_stage_out)

    def _globus_stage_out(self, globus_ep, inputs=[]):
        file = inputs[0]
        src_path = os.path.join(globus_ep['endpoint_path'], file.filename)

        self.initialize_globus()

        self.globus.transfer_file(
            globus_ep['endpoint_uuid'], file.netloc,
            src_path, file.path
        )
