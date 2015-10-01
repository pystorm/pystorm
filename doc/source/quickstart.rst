Quickstart
==========

Dependencies
------------

Spouts and Bolts
----------------

The general flow for creating new spouts and bolts using pystorm is to add
them to your ``src`` folder and update the corresponding topology definition.

Let's create a spout that emits sentences until the end of time:

.. code-block:: python

    import itertools

    from pystorm.spout import Spout


    class SentenceSpout(Spout):

        def initialize(self, stormconf, context):
            self.sentences = [
                "She advised him to take a long holiday, so he immediately quit work and took a trip around the world",
                "I was very glad to get a present from her",
                "He will be here in half an hour",
                "She saw him eating a sandwich",
            ]
            self.sentences = itertools.cycle(self.sentences)

        def next_tuple(self):
            sentence = next(self.sentences)
            self.emit([sentence])

        def ack(self, tup_id):
            pass  # if a tuple is processed properly, do nothing

        def fail(self, tup_id):
            pass  # if a tuple fails to process, do nothing

The magic in the code above happens in the ``initialize()`` and
``next_tuple()`` functions.  Once the spout enters the main run loop,
pystorm will call your spout's ``initialize()`` method.
After initialization is complete, pystorm will continually call the spout's
``next_tuple()`` method where you're expected to emit tuples that match
whatever you've defined in your topology definition.

Now let's create a bolt that takes in sentences, and spits out words:

.. code-block:: python

    import re

    from pystorm.bolt import Bolt

    class SentenceSplitterBolt(Bolt):

        def process(self, tup):
            sentence = tup.values[0]  # extract the sentence
            sentence = re.sub(r"[,.;!\?]", "", sentence)  # get rid of punctuation
            words = [[word.strip()] for word in sentence.split(" ") if word.strip()]
            if not words:
                # no words to process in the sentence, fail the tuple
                self.fail(tup)
                return

            self.emit_many(words)
            # tuple acknowledgement is handled automatically

The bolt implementation is even simpler. We simply override the default
``process()`` method which pystorm calls when a tuple has been emitted by
an incoming spout or bolt. You are welcome to do whatever processing you would
like in this method and can further emit tuples or not depending on the purpose
of your bolt.

In the ``SentenceSplitterBolt`` above, we have decided to use the
``emit_many()`` method instead of ``emit()`` which is a bit more efficient when
sending a larger number of tuples to Storm.

If your ``process()`` method completes without raising an Exception, pystorm
will automatically ensure any emits you have are anchored to the current tuple
being processed and acknowledged after ``process()`` completes.

If an Exception is raised while ``process()`` is called, pystorm
automatically fails the current tuple prior to killing the Python process.

Failed Tuples
^^^^^^^^^^^^^

In the example above, we added the ability to fail a sentence tuple if it did
not provide any words. What happens when we fail a tuple? Storm will send a
"fail" message back to the spout where the tuple originated from (in this case
``SentenceSpout``) and pystorm calls the spout's
:meth:`~pystorm.spout.Spout.fail` method. It's then up to your spout
implementation to decide what to do. A spout could retry a failed tuple, send
an error message, or kill the topology.

Bolt Configuration Options
^^^^^^^^^^^^^^^^^^^^^^^^^^

You can disable the automatic acknowleding, anchoring or failing of tuples by
adding class variables set to false for: ``auto_ack``, ``auto_anchor`` or
``auto_fail``.  All three options are documented in
:class:`pystorm.bolt.Bolt`.

**Example**:

.. code-block:: python

    from pystorm.bolt import Bolt

    class MyBolt(Bolt):

        auto_ack = False
        auto_fail = False

        def process(self, tup):
            # do stuff...
            if error:
              self.fail(tup)  # perform failure manually
            self.ack(tup)  # perform acknowledgement manually

Handling Tick Tuples
^^^^^^^^^^^^^^^^^^^^

Ticks tuples are built into Storm to provide some simple forms of
cron-like behaviour without actually having to use cron. You can
receive and react to tick tuples as timer events with your python
bolts using pystorm too.

The first step is to override ``process_tick()`` in your custom
Bolt class. Once this is overridden, you can set the storm option
``topology.tick.tuple.freq.secs=<frequency>`` to cause a tick tuple
to be emitted every ``<frequency>`` seconds.

You can see the full docs for ``process_tick()`` in
:class:`pystorm.bolt.Bolt`.

**Example**:

.. code-block:: python

    from pystorm.bolt import Bolt

    class MyBolt(Bolt):

        def process_tick(self, freq):
            # An action we want to perform at some regular interval...
            self.flush_old_state()

Then, for example, to cause ``process_tick()`` to be called every
2 seconds on all of your bolts that override it, you can launch
your topology under ``sparse run`` by setting the appropriate -o
option and value as in the following example:

.. code-block:: bash

    $ sparse run -o "topology.tick.tuple.freq.secs=2" ...
