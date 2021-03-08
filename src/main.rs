extern crate timely;

use std::collections::HashMap;

use timely::dataflow::operators::CapabilityRef;
use timely::dataflow::{
  channels::pact::Exchange,
  operators::{Inspect, Map, Operator, Probe},
};
use timely::dataflow::{InputHandle, ProbeHandle};

// TODO: https://timelydataflow.github.io/timely-dataflow/chapter_3/chapter_3.html
// TODO: https://timelydataflow.github.io/timely-dataflow/chapter_2/chapter_2_4.html#frontiered-operators
fn main() {
  timely::execute_from_args(std::env::args(), |worker| {
    let mut input = InputHandle::new();
    let mut probe = ProbeHandle::new();

    worker.dataflow(|scope| {
      input
        .to_stream(scope)
        .flat_map(|(text, diff): (String, i64)| {
          text
            .split_whitespace()
            .map(move |word| (word.to_string(), diff))
            .collect::<Vec<_>>()
        })
        .unary_frontier(
          Exchange::new(|x: &(String, i64)| (x.0).len() as u64),
          "WordCount",
          |_capability, _operator_info| {
            let mut queues = HashMap::new();
            let mut counts = HashMap::new();
            let mut buffer = Vec::new();

            move |input, output| {
              // for each input batch, stash it at `time`.
              while let Some((time, data)) = input.next() {
                let time: CapabilityRef<'_, usize> = time;

                queues
                  .entry(time.retain())
                  .or_insert(Vec::new())
                  .extend(data.replace(Vec::new()));
              }

              // enable each stashed time if ready.
              for (time, vals) in queues.iter_mut() {
                if !input.frontier().less_equal(time.time()) {
                  let vals = std::mem::replace(vals, Vec::new());
                  buffer.push((time.clone(), vals));
                }
              }

              queues.retain(|_time, vals| vals.len() > 0);
              buffer.sort_by(|x, y| (x.0).time().cmp(&(y.0).time()));

              for (time, mut vals) in buffer.drain(..) {
                let mut session = output.session(&time);
                for (word, diff) in vals.drain(..) {
                  let entry = counts.entry(word.clone()).or_insert(0i64);
                  *entry += diff;
                  session.give((word, *entry));
                }
              }
            }
          },
        )
        .inspect(|x| println!("seen: {:?}", x))
        .probe_with(&mut probe);
    });

    for round in 0..5 {
      input.send(("this round".to_owned(), 1));

      input.advance_to(round + 1);
      while probe.less_than(input.time()) {
        worker.step();
      }
    }
  })
  .unwrap();
}
