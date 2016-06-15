//! Controls the execution of a mapreduce instance.

use phases::output::{SinkGenerator, open_reduce_inputs, get_reduce_output_name};
use formats::writelog::WriteLogGenerator;
use input_cache::InputCache;
use phases::map::MapPartition;
use mapreducer::{Mapper, Reducer, Sharder};
use parameters::MRParameters;
use record_types::Record;
use phases::reduce::ReducePartition;

use std::sync::mpsc::sync_channel;

extern crate scoped_threadpool;
use self::scoped_threadpool::Pool;

pub struct MRController<M: Mapper, R: Reducer, S: Sharder> {
    params: MRParameters,
    m: M,
    r: R,
    s: S,

    // How many map partitions have been run?
    map_partitions_run: usize,
}


impl<M: Mapper, R: Reducer, S: Sharder> MRController<M, R, S> {
    /// Create a new mapreduce instance and execute it immediately.
    ///
    /// You can use `DefaultSharder` as `sharder` argument.
    pub fn run<In: Iterator<Item = Record>, Out: SinkGenerator>(mapper: M,
                                                                reducer: R,
                                                                sharder: S,
                                                                params: MRParameters,
                                                                inp: In,
                                                                out: Out) {
        let mut controller = MRController {
            params: params,
            m: mapper,
            r: reducer,
            s: sharder,
            map_partitions_run: 0,
        };
        controller.run_map(inp);
        controller.run_reduce(out);
        controller.clean_up();
    }

    fn run_map<In: Iterator<Item = Record>>(&mut self, mut input: In) {
        let mut pool = Pool::new(self.params.mappers as u32);
        // Create channels for worker synchronization; this ensures that there are only as many
        // mapper threads running as specified.
        let (send, recv) = sync_channel(self.params.mappers);

        for _ in 0..self.params.mappers {
            let _ = send.send(true);
        }

        pool.scoped(move |scope| {
            loop {
                let _ = recv.recv();

                let m = self.m.clone();
                let s = self.s.clone();
                // Can't necessarily send the input handle to the mapper thread, therefore read
                // input before spawn.
                let inp = MRController::<M, R, S>::read_map_input(&mut input,
                                                                  self.params.map_partition_size);

                if inp.len() == 0 {
                    break;
                }

                let params = self.params.clone().set_shard_id(self.map_partitions_run as usize);
                let done = send.clone();

                scope.execute(move || {
                    MRController::<M, R, S>::map_runner(m, s, params, inp);
                    let _ = done.send(true);
                });
                self.map_partitions_run += 1;
            }

            scope.join_all();
        });
    }

    fn map_runner(mapper: M, sharder: S, params: MRParameters, inp: InputCache) {
        if inp.len() == 0 {
            return;
        }
        let intermed_out = WriteLogGenerator::new();
        let map_part = MapPartition::_new(params, inp, mapper, sharder, intermed_out);
        map_part._run();
    }

    fn read_map_input<In: Iterator<Item = Record>>(it: &mut In, approx_bytes: usize) -> InputCache {
        let inp_cache = InputCache::from_iter(8192, approx_bytes, it);
        inp_cache
    }


    fn run_reduce<Out: SinkGenerator>(&self, outp: Out) {
        let mut pool = Pool::new(self.params.reducers as u32);

        pool.scoped(move |scope| {
            for i in 0..self.params.reducers {
                let r = self.r.clone();
                let params = self.params.clone().set_shard_id(i);
                let map_partitions = self.map_partitions_run;
                let output = outp.clone();

                scope.execute(move || {
                    let inputs = open_reduce_inputs(&params.map_output_location, map_partitions, i);
                    let output = output.new_output(&get_reduce_output_name(&params));
                    let reduce_part = ReducePartition::new(r, params, inputs, output);
                    reduce_part._run();
                });
            }
        });
    }

    fn clean_up(&self) {
        use std::fs;
        use std::fmt;

        if !self.params.keep_temp_files {
            for mpart in 0..self.map_partitions_run {
                for rshard in 0..self.params.reducers {
                    let name = fmt::format(format_args!("{}{}.{}",
                                                        self.params.map_output_location,
                                                        mpart,
                                                        rshard));
                    let _ = fs::remove_file(name);
                }
            }
        }
    }
}
