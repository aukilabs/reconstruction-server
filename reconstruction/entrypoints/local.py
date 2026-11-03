"""Local refinement: refine each scan in <job root>/datasets into <output>/local/<scan>/sfm."""
from concurrent.futures import ProcessPoolExecutor

from reconstruction.entrypoints import _refinement
from reconstruction.pipeline.local.refine import refine_dataset


def process_local_refinement(args, layout, scan, worker_pool=None):
    """Process local refinement for a single scan.

    Args:
        args: Command line arguments
        layout: JobLayout of the job
        scan: Name of the scan to process
    """
    return refine_dataset(
        layout,
        scan,
        every_nth_image=1,
        remove_outputs=False,
        domain_id=args.domain_id,
        job_id=args.job_id,
        log_level=args.log_level,
        log_format=args.log_format,
        pool_executor=worker_pool
    )


def local_main_wrapper(args, layout, logger):
    """Run local refinement on all scans.

    Args:
        args: Command line arguments
        layout: JobLayout of the job
        logger: Logger instance
    """
    logger.info("--------------------------------")
    logger.info(f"Running local refinement on {len(args.scans)} scans")
    logger.info(f"Job root path: {args.job_root_path}")
    logger.info(f"Output path: {args.output_path}")
    logger.info(f"Scans: {args.scans}")
    logger.info("--------------------------------")

    def process_all(pool_executor=None):
        futures = []
        for scan in args.scans:
            logger.info(f"Refining scan {scan}...")
            future = process_local_refinement(args, layout, scan, pool_executor)
            if future:
                logger.info(f"Finished part 1 of local refinement for scan {scan}. Part 2 queued to worker pool.")
                futures.append(future)
                future.add_done_callback(lambda f: logger.info(
                    f"Finished refining scan {scan}" if not f.exception()
                    else f"Failed to refine scan {scan}: {f.exception()}"
                ))
            else:
                logger.info(f"Done refining scan {scan}")

            # Abort early if any refinement thread throws an exception
            if futures:
                for f in futures:
                    if f.done() and f.exception():
                        for f2 in futures:
                            if not f2.done():
                                f2.cancel()
                        f.result() # raises the exception (with callstack)

        # Wait for all threads. Does nothing if running without pool.
        for f in futures:
            f.result() # waits, and raises any exception from the worker (with full call stack)

    if args.local_refinement_workers and args.local_refinement_workers >= 1:
        with ProcessPoolExecutor(max_workers=args.local_refinement_workers) as pool_executor:
            process_all(pool_executor)
            pool_executor.shutdown(wait=True)
    else:
        process_all()


def parse_args(argv, prog=None):
    return _refinement.parse_args("local", argv, prog)


def main(args):
    _refinement.run(args, local_main_wrapper)
