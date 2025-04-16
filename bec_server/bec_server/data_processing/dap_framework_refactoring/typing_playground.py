from typing import TypeVar, Literal, reveal_type, Generic
from dataclasses import dataclass
from functools import reduce
from operator import add

Dims = TypeVar("Dims", bound=Literal[1,2,3], contravariant=True)
Update = TypeVar("Update", Literal["add"], Literal["replace"], Literal["append"], contravariant=True)
XAxis = TypeVar("XAxis", bool, None) # bool means the axis must be present or not be present, none means the function can handle either

@dataclass
class BlockSpec(Generic[Dims, Update, XAxis]): # this is fine with no defaults
    ndims: Dims
    update: Update
    x_axis_present: XAxis

test1 = BlockSpec(4, "replace", None)          # 4 is rejected by pyright for not being 1, 2 or 3
test1 = BlockSpec(2, "backwards", True)        # "backwards" is rejected by pyright for not being an update type literal
test2 = BlockSpec(2, "replace", None)          # no problem
reveal_type(test2)                             # type of test2 is BlockSpec[Literal[2],Literal["replace"],None]
test2 = BlockSpec(1, "add", None)              # no problem

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
    return DAPMessage(data = list(reversed(msg.data)), spec=msg.spec) # no problem

message_with_1d_data = DAPMessage(data=[1,0,1,0,1,0,1], spec=BlockSpec(1, "replace", None))
message_with_2d_data = DAPMessage(data=[[1,0,0],[0,1,0],[0,0,1]], spec=BlockSpec(2, "replace", None))

reverse_1d(message_with_1d_data) # allowed
reverse_1d(message_with_2d_data) # not allowed! Argument of type DAPMessage[TwoDReplace] can't be assigned to parameter of type DAPMessage[OneDReplace]



ONE_D_REPLACE = OneDReplace(1,"replace", None)

def hstack[T: int|float](l: list[list[T]]) -> list[T]:
    return reduce(add, l)

def flatten(msg: DAPMessage[TwoDReplace]) -> DAPMessage[OneDReplace]:
    return  DAPMessage(data = hstack(msg.data), spec=ONE_D_REPLACE)

flatten(message_with_1d_data) # not allowed
flatten(message_with_2d_data) # allowed
reverse_1d(message_with_2d_data) # not allowed
reverse_1d(flatten(message_with_2d_data)) # allowed !

"""
So far so good, we can approximate something like what we want to do in this way, for blocks which
we develop ourselves. Maybe users could even do something with a bunch of pre-defined specs like
above, but they probably wouldn't want to.

But what if we want to support more than one kind of input...
"""

AnyDMessage = TypeVar("AnyDMessage", DAPMessage[OneDReplace], DAPMessage[TwoDReplace])
def reverse_any_d(msg: AnyDMessage ) -> AnyDMessage:
    # We want to return a reversed array of the same dimensions as it came in, this should be easy...
    out_data = list(reversed(msg.data)) # Just do an operation which doesn't change the dimensions
    spec = msg.spec # and keep the input spec
    result_message = DAPMessage(spec=spec, data=out_data)
    reveal_type(result_message) # DAPMessage[OneDReplace | TwoDReplace]
    return result_message # Type "DAPMessage[OneDReplace | TwoDReplace]" is not assignable to type "AnyDMessage@reverse_any_d"
    # Because the result message is obviously the input type, you would expect this to work, but it is not supported
    # We couldn't track that spec should be type S@AnyDMessage@reverse_any_d
    # Why is this not possible? 
    # https://microsoft.github.io/pyright/#/type-concepts-advanced?id=value-constrained-type-variables
    



