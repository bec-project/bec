from dataclasses import dataclass
from functools import reduce
from operator import add
from typing import Callable, Concatenate, Final, Generic, Literal, ParamSpec, TypeVar, reveal_type

Dims = TypeVar("Dims", bound=Literal[1, 2, 3], contravariant=True)
Update = TypeVar(
    "Update", Literal["add"], Literal["replace"], Literal["append"], contravariant=True
)
XAxis = TypeVar(
    "XAxis", bool, None
)  # bool means the axis must be present or not be present, none means the function can handle either


@dataclass
class BlockSpec(Generic[Dims, Update, XAxis]):  # this is fine with no defaults
    ndims: Dims
    update: Update
    x_axis_present: XAxis


test1 = BlockSpec(4, "replace", None)  # 4 is rejected by pyright for not being 1, 2 or 3
test1 = BlockSpec(
    2, "backwards", True
)  # "backwards" is rejected by pyright for not being an update type literal
test2 = BlockSpec(2, "replace", None)  # no problem
reveal_type(test2)  # type of test2 is BlockSpec[Literal[2],Literal["replace"],None]
test2 = BlockSpec(1, "add", None)  # no problem


@dataclass
class DAPMessage[S: BlockSpec]:
    spec: S
    data: list


"""
Let's explore the test case of limiting by dimensions, but these examples would easily apply to anything else
"""

OneDReplace = BlockSpec[Literal[1], Literal["replace"], None]
TwoDReplace = BlockSpec[Literal[2], Literal["replace"], None]


def reverse_1d(msg: DAPMessage[OneDReplace]) -> DAPMessage[OneDReplace]:
    return DAPMessage(data=list(reversed(msg.data)), spec=msg.spec)  # no problem


message_with_1d_data = DAPMessage(data=[1, 0, 1, 0, 1, 0, 1], spec=BlockSpec(1, "replace", None))
message_with_2d_data = DAPMessage(
    data=[[1, 0, 0], [0, 1, 0], [0, 0, 1]], spec=BlockSpec(2, "replace", None)
)

reverse_1d(message_with_1d_data)  # allowed
reverse_1d(
    message_with_2d_data
)  # not allowed! Argument of type DAPMessage[TwoDReplace] can't be assigned to parameter of type DAPMessage[OneDReplace]


ONE_D_REPLACE = OneDReplace(1, "replace", None)
TWO_D_REPLACE = TwoDReplace(2, "replace", None)


def hstack[T: int | float](l: list[list[T]]) -> list[T]:
    return reduce(add, l)


def flatten(msg: DAPMessage[TwoDReplace]) -> DAPMessage[OneDReplace]:
    return DAPMessage(data=hstack(msg.data), spec=ONE_D_REPLACE)


def transpose(msg: DAPMessage[TwoDReplace]) -> DAPMessage[TwoDReplace]:
    return DAPMessage(data=list(map(list, zip(*msg.data))), spec=TWO_D_REPLACE)


flatten(message_with_1d_data)  # not allowed
flatten(message_with_2d_data)  # allowed
reverse_1d(message_with_2d_data)  # not allowed
reverse_1d(flatten(message_with_2d_data))  # allowed !

"""
So far so good, we can approximate something like what we want to do in this way, for blocks which
we develop ourselves. Maybe users could even do something with a bunch of pre-defined specs like
above, but they probably wouldn't want to.

But what if we want to support more than one kind of input...
"""

AnyDMessage = TypeVar("AnyDMessage", DAPMessage[OneDReplace], DAPMessage[TwoDReplace])


def reverse_any_d(msg: AnyDMessage) -> AnyDMessage:
    # We want to return a reversed array of the same dimensions as it came in, this should be easy...
    out_data = list(reversed(msg.data))  # Just do an operation which doesn't change the dimensions
    spec = msg.spec  # and keep the input spec
    result_message = DAPMessage(spec=spec, data=out_data)
    reveal_type(result_message)  # DAPMessage[OneDReplace | TwoDReplace]
    return result_message  # Type "DAPMessage[OneDReplace | TwoDReplace]" is not assignable to type "AnyDMessage@reverse_any_d"
    # Because the result message is obviously the input type, you would expect this to work, but it is not supported
    # We couldn't track that spec should be type S@AnyDMessage@reverse_any_d
    # Why is this not possible?
    # https://microsoft.github.io/pyright/#/type-concepts-advanced?id=value-constrained-type-variables


"""
What if we wanted to have a default argument?
"""


@dataclass
class BlockSpecWithDefaults(BlockSpec[Dims, Update, XAxis]):  # this is fine with no defaults
    ndims: Dims
    update: Update
    x_axis_present: XAxis = None
    # Type "None" is not assignable to declared type "XAxis@BlockSpecWithDefaults"
    # Why not? It can clearly be None, since that is a bound we defined


"""
Initialising these still works, though:
"""

bwd = BlockSpecWithDefaults(2, "replace")
reveal_type(bwd)  # BlockSpecWithDefaults[Literal[2], Literal['replace'], None]
bwd_t = BlockSpecWithDefaults(2, "replace", True)
reveal_type(bwd_t)  # BlockSpecWithDefaults[Literal[2], Literal['replace'], bool]

"""
A typed workflow class:
"""


class WorkFlowStartSentinel: ...


class WorkFlowStopSentinel: ...


WORKFLOW_START: Final = WorkFlowStartSentinel()
WORKFLOW_END: Final = WorkFlowStopSentinel()


class Workflow[S: BlockSpec]:
    def __init__(
        self,
        block: Callable[[DAPMessage], DAPMessage[S]],
        previous: "Workflow | WorkFlowStartSentinel" = WORKFLOW_START,
    ):
        self._block = block
        self._previous = previous
        self._next: Workflow | WorkFlowStopSentinel = WORKFLOW_END

    def add(self, block: Callable[[DAPMessage[S]], DAPMessage]) -> "Workflow":
        new = Workflow(block, self)
        new._previous = self
        self._next = new
        return new

    def serialised(self):
        return self._block.__name__

    def print_workflow(self):
        print("\nWorkflow:")
        print("---")
        node: Workflow = self
        while not isinstance(node._previous, WorkFlowStartSentinel):
            node = node._previous
        while not isinstance(node._next, WorkFlowStopSentinel):
            print(f"    {node.serialised()}")
            node = node._next
        print("---\n")


wf1 = Workflow(transpose).add(flatten).add(reverse_1d)  # allowed
wf2 = Workflow(transpose).add(reverse_1d)  # not allowed, second step expects 1D data

"""
You can even type the additional arguments to some degree
"""

P = ParamSpec("P")
PNext = ParamSpec("PNext")


class WorkflowWithParams[T: BlockSpec, S: BlockSpec]:
    def __init__(
        self,
        block: Callable[Concatenate[DAPMessage[T], P], DAPMessage[S]],
        *args: P.args,
        **kwargs: P.kwargs,
    ):
        self._block = block
        self._previous: WorkflowWithParams | WorkFlowStartSentinel = WORKFLOW_START
        self._next: WorkflowWithParams | WorkFlowStopSentinel = WORKFLOW_END
        self._args = args
        self._kwargs = kwargs

    def add(
        self,
        block: Callable[Concatenate[DAPMessage[S], PNext], DAPMessage],
        *args: PNext.args,
        **kwargs: PNext.kwargs,
    ) -> "WorkflowWithParams":
        new = WorkflowWithParams(block, *args, **kwargs)
        new._previous = self
        self._next = new
        return new

    def serialised(self):
        return f"{self._block.__name__}, with args {self._args} and kwargs {self._kwargs}"

    def print_workflow(self):
        print("\nWorkflow:")
        print("---")
        node: WorkflowWithParams = self
        while node._previous is not WORKFLOW_START:
            node = node._previous  # type: ignore
        while node is not WORKFLOW_END:
            print(f"    {node.serialised()}")
            node = node._next  # type: ignore
        print("---\n")

    def execute_workflow_from_here(self, msg: DAPMessage[T]):
        res = self._block(msg, *self._args, **self._kwargs)
        print(f"Step: applying {self._block.__name__}")
        print(f"In data: {msg.data}")
        print(f"Out message: {res}\n")
        if self._next is WORKFLOW_END or isinstance(self._next, WorkFlowStopSentinel):
            return res
        return self._next.execute_workflow_from_here(res)

    def execute_workflow_from_start(self, msg: DAPMessage):
        # not sure how one could type this unless each node tracks the start and saves that type
        # (which might be the better approach anyway since we only use _previous to find the head)
        # but that would fail for multiple starts
        print("executing workflow:")
        node: WorkflowWithParams = self
        while not isinstance(node._previous, WorkFlowStartSentinel):
            node = node._previous
        return node.execute_workflow_from_here(msg)


def scale(msg: DAPMessage[OneDReplace], factor: float) -> DAPMessage[OneDReplace]:
    return DAPMessage(data=[i * factor for i in msg.data], spec=ONE_D_REPLACE)


def replace_values(
    msg: DAPMessage[OneDReplace], from_value: float, to_value: float
) -> DAPMessage[OneDReplace]:
    return DAPMessage(
        data=[i if i != from_value else to_value for i in msg.data], spec=ONE_D_REPLACE
    )


wfp1 = WorkflowWithParams(scale, factor=0.1)  # allowed
wfp2 = WorkflowWithParams(scale, 0.1)  # allowed
# not allowed, argument of type "Literal['half']" cannot be assigned to parameter "factor" of type "float"
wfp3 = WorkflowWithParams(scale, "half")

"""Thanks to ParamSpec, .add() knows it needs the additional args and kwargs which the first argument function
also takes, following the message"""

wfp4 = WorkflowWithParams(flatten).add(replace_values, 0, 0.001).add(scale, 0.1).add(reverse_1d)


if __name__ == "__main__":
    wf1.print_workflow()
    wfp4.print_workflow()
    wfp4.execute_workflow_from_start(
        DAPMessage(spec=TWO_D_REPLACE, data=[[1, 2, 3, 0], [4, 5, 6, 0], [7, 8, 9, 0]])
    )
