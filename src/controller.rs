//! Controls the execution of a mapreduce instance.

use formats::util::{SinkGenerator, RecordReadIterator};
use formats::writelog::{WriteLogGenerator, WriteLogReader};
use map::MapPartition;
use mapreducer::MapReducer;
use parameters::MRParameters;
use record_types::Record;
use reduce::ReducePartition;

use std::collections::LinkedList;
use std::sync::mpsc::sync_channel;

extern crate scoped_threadpool;
use self::scoped_threadpool::Pool;

pub struct MRController<MR: MapReducer> {
    params: MRParameters,
    mr: MR,

    // How many map partitions have been run?
    map_partitions_run: usize,
}

/// Calculates the name of a reduce output shard from the parameters.
fn get_reduce_output_name(params: &MRParameters) -> String {
    use std::fmt;
    let mut name = String::new();
    name.push_str(&params.reduce_output_shard_prefix[..]);
    name.push_str(&fmt::format(format_args!("{}", params.shard_id))[..]);
    name
}

fn open_reduce_inputs(params: &MRParameters,
                      partitions: usize,
                      shard: usize)
                      -> Vec<RecordReadIterator<WriteLogReader>> {
    use std::fmt;
    let mut inputs = Vec::new();

    for part in 0..partitions {
        let name = fmt::format(format_args!("{}{}.{}", params.map_output_location, shard, part));
        let wlg_reader = WriteLogReader::new_from_file(&name).unwrap();
        inputs.push(RecordReadIterator::new(wlg_reader));
    }
    inputs
}


impl<MR: MapReducer + Send> MRController<MR> {
    /// Create a new mapreduce instance and execute it immediately.
    pub fn run<In: Iterator<Item = Record>, Out: SinkGenerator>(mr: MR,
                                                                params: MRParameters,
                                                                inp: In,
                                                                out: Out) {
        let mut controller = MRController {
            params: params,
            mr: mr,
            map_partitions_run: 0,
        };
        controller.run_map(inp);
        controller.run_reduce(out);
    }

    fn map_runner(mr: MR, params: MRParameters, inp: LinkedList<Record>) {
        if inp.len() == 0 {
            return;
        }
        let intermed_out = WriteLogGenerator::new();
        let map_part = MapPartition::_new(params, inp.into_iter(), mr, intermed_out);
        map_part._run();
    }

    fn read_map_input<In: Iterator<Item = Record>>(it: &mut In,
                                                   approx_bytes: usize)
                                                   -> LinkedList<Record> {
        let mut ll = LinkedList::new();
        let mut bytes_read: usize = 0;

        for r in it {
            bytes_read += r.key.len() + r.value.len() + 4; // Heuristics :P
            ll.push_back(r);

            if bytes_read > approx_bytes {
                break;
            }
        }
        ll
    }

    fn run_map<In: Iterator<Item = Record>>(&mut self, mut input: In) {
        let mut pool = Pool::new(self.params.mappers as u32);
        let (send, recv) = sync_channel(self.params.mappers);

        for _ in 0..self.params.mappers {
            let _ = send.send(true);
        }

        pool.scoped(move |scope| {
            loop {
                let _ = recv.recv();

                let mr = self.mr.clone();
                let inp = MRController::<MR>::read_map_input(&mut input,
                                                             self.params.map_partition_size);

                if inp.len() == 0 {
                    break;
                }

                let params = self.params.clone().set_shard_id(self.map_partitions_run as usize);
                let done = send.clone();

                scope.execute(move || {
                    MRController::map_runner(mr, params, inp);
                    let _ = done.send(true);
                });
                self.map_partitions_run += 1;
            }

            scope.join_all();
        });
    }

    fn run_reduce<Out: SinkGenerator>(&self, outp: Out) {
        let mut pool = Pool::new(self.params.reducers as u32);

        pool.scoped(move |scope| {
            for i in 0..self.params.reducers {
                let mr = self.mr.clone();
                let params = self.params.clone().set_shard_id(i);
                let map_partitions = self.map_partitions_run;
                let output = outp.clone();

                scope.execute(move || {
                    let inputs = open_reduce_inputs(&params, map_partitions, i);
                    let output = output.new_output(&get_reduce_output_name(&params));
                    let reduce_part = ReducePartition::new(mr, params, inputs, output);
                    reduce_part._run();
                });
            }
        });
    }
}
