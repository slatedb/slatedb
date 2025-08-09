---
applyTo: "**/*.fizz"
---
This file has copilot instructions for `.fizz` files.

# Fizz File Structure
The language structure is,

1. .fizz file can optionally have a yaml frontmatter surrounded by `---` at the start and again at end of the frontmatter config. Followed by the actual fizz specification. The allowed configs will be specified later.
2. The rest of the instructions will talk only about the fizz part of the file.
3. The fizz file can have,
   3a. Global top level statements (usually to define global constants. They can be accessed with the global keyword unlike python/starlark)
   3b. functions,
   3c. actions,
   3d. assertions
   3e. roles (similar to class).

4. The top level statements will be executed first. The constants will be recorded and used later. (Only constants should be at the global level. For global variables, initialize them in top level action Init)
5. Functions are similar to starlark functions, but defined using the keyword `func`. functions can either be top level or part of a role. When part of a role, unlike typical python `self` is implicit like Java's this, so no need to add self as the first parameter. For top level functions it behaves almost similar to starlarks function.
   5a. Functions accept an optional flow modifier. Flow modifier can either be `atomic` or `serial`(default) . Since serial is the default, the convention is to not specify serial and include atomic only when needed.
   (We will see more about the flow modifiers later). The syntax will look like
```
atomic func do_something():
    pass

```
5b. functions can also return like typical starlark/python. The body of the method is almost starlark. (A few minor changes will be explained later). For most part just use starlark statements and expressions.

5c. functions are called exactly like any other starlark function from other statements.

6. actions are specified using the `action` keyword. actions are like functions. The only difference is actions are not called from other fizz code, but automatically by the model checker following some rules.
   6a. actions are kind of like TLA+ actions as this is a formal language.
   6b. actions take optional flow modifiers - `atomic` vs `serial`(default) and optional fairness modifiers (default empty implies unfair, `fair` or `fair<weak> for weak fairness and `fair<strong>` for strong fairness. Fairness is a LTL concept similar to TLA+, for the syntax knowing that exists is good enough for now).
6c. actions do not take any parameters, so actions do not have a parameter list and not even the empty parenthesis.
6d. actions can optional return similar to functions. (Even though no one is using it, the return values will be used only for additional features like visualizations and testcase generation)
6e. actions can have a body similar to functions. The body of the action is almost starlark. (A few minor changes will be explained later). For most part just use starlark statements and expressions.
6f. there is a special action named `Init`, that will be called once at the beginning of the model checker (after executing the top level statements). This is similar to the `Init` action in TLA+.
   6g. all the variables defined in the scope of the Init action at the end of the Init action will be considered as the initial state of the model checker. they will be part of the state.
   6i. variables defined in other actions or functions are not part of the state. So Init action is special in this regard.
   6h. actions are called automatically by the model checker following some rules. (Similar to adding the action to the Next state relation in TLA+)

7. assertions are like functions without `assertion`, but they are not called from other fizz code, but automatically by the model checker following some rules.
   7a. assertions are like TLA+ invariants. They are checked at every step of the model checker.
   7b. assertions are specified using `assertion` keyword and but the syntax is similar to functions/actions.
   7c. assertions do not take any parameters either, so no parameter list and no empty parenthesis.
   7d. assertions can have modifiers and the order matters. The only modifier combinations are `always`, `always eventually`, `eventually always`, `exists` or `transition`. At least one of these 4 are required.
   7e. assertions can have a body similar to functions. The body of the assertion is actually starlark. they can call other functions as well.
   7f. return `True` to imply the assertion is satisfied and `False` to imply the assertion is violated. (The model checker will use modifiers to decidd if it is a safety or liveness property and check them accordingly)
   7g. only transition assertion takes 2 arguments, although can be named anything, usually named `before` and `after` by convention, and all the state variables will be accessible as property of these two. Transition assertions must be stutter invariant. To make it easier, if no state variables changed, even if the thread local variables changed, fizzbee will assume the invariant is satisfied. 

8. roles are like classes in python. They are defined using the `role` keyword.
   8a. roles can have functions, actions and assertions.
   8b. roles can have a special action named `Init`. Here, it will act like a class's construction (__init__) function. It will be called for each role instance once when the role is instantiated (similar to class instantiation calling __init__ function in standard python).
   8c. For role's state, only those roles prefixed with `self` keyword are considered as part of the state. The rest are not part of the state.
   8d. For role's actions also, there are no parameter list or parenthesis etc. The `self` will be injected automatically.
   8e. For role's functions, the `self` is implicit like Java's `this` and no need to add `self` as the first parameter. Other parameters can be added as needed.
   8f. Similar to top level functions and actions, the flow and fairness modifiers can be added to role's functions and actions as well.
   8g. `assertions` are not supported yet within each role, but would be added soon. The rules are same as top level assertions.
   8i. The roles can have decorators (or annotations). At present, only one annotation is supported called `state` that will be used to define which state variables of the roles are `durable` vs `ephemeral`. The syntax is similar to python's decorators.
   8j. By default all of roles states are considered `durable`. If you want to mark some of them as `ephemeral`, you can use the `state` decorator.
   8k1. @state(ephemeral=['field1', 'field2']) where field1 and field2 are the state variables initialized using `self.field1` or `self.field2`. All other fields not marked ephemeral are considered durable.
   8k2. If most states are ephemeral, you can use @state(durable=['field1', 'field2']) to mark the durable fields. All other fields not marked durable are considered ephemeral.
   8k3. Specifying both `ephemeral` and `durable` is an error.

9. Statements within the functions, actions, assertions are almost starlark.
   9a. basic python/starlark expressions and statements are executed using starlark engine. Each basic statement will always be considered a single atomic (indivisible) statement, unlike regular programming. So `a = a + 1`, is indivisible.
   9b. When there are multiple statements, they are executed one after another using the starlark engine, continuing to pass the updated state and local variables from one statement to the next.
   9c. For the compound statements like if/elif/else, for, while, or function calls etc, they are not directly executed by starlark, instead fizzbee interpreter parses them and executes them following the standard semantics.
   9d. FizzBee interpreter executes the expr in if,elif,else one by one and when the expr has Truth value True, that branch executes (or else gets executed) for,while are executed similarly once for each iteration.
   9e. Each block can optionally have flow modifiers. For blocks the flow modifiers can be one of `atomic`, `serial`, `parallel`, `oneof`. The default is inherited from parent block all the way to the top level `action` or `func`'s flow modifer where the default is `serial`.
   9f. Block modifiers defines the fork and yield points for the model checker. FizzBee is an exhaustive explicit model checker. So it will check the state space exhaustively evaluating each fork point and exploring that path of the state space, and similarly at each yield point, it will explore, what happens if the thread continues without yielding, or if the thread gets preempted and another existing thread gets scheduled or if a new action is scheduled.
   9g. Each stat

10: Flow modifier: `atomic`
10a. `atomic` modifier is used to specify that the block is atomic. When there are multiple statements are part of the atomic block, all these statements will be executed as a single indivisible step. So other threads will not see the partially executed statements. (that means, this will be like `atomic` and `isolated` in ACID terms)

11: Flow modifier: `oneof`
11a. If a block with `oneof` modifier has 'k' statements, the model checker will check `k` different paths assuming either one of the k statements were executed. Note: The statement could itself be a block.

12: Flow modifier: `serial`
12a. `serial` modifier implies, if there are multiple statements, after each statement is executed, there will be an implicit yield point. So the model checker will explore cases where the thread continued without yielding, yield and schedule a different thread, or schedule a completely new execution.

13: Flow modifier: `parallel`
13a. `parallel` modifier implies, all the statements within the block will be executed concurrently. Each of the statement themselves could be blocks with their own flow modifiers. The model checker will explore all possible interleavings of the statements.

14: Enabling a state transition:
When an action is executed, if there is at least one simple python statement (not conditionals, loops, break, continue, return etc) then the transition is considered enabled.
To conditionally disable a current transition, you can use a `require` statement. The syntax is `require <starlark_expression>`. If the starlark_expression is False, then the transition is disabled and the model checker will not explore that path. The starlark_expression can be any starlark expression that evaluates to a boolean.
To unconditionaly enable a current transition, use `pass` statement. This is similar to `skip` in TLA+.

15. There are many keywords like role, action, func, assertion, always, eventually, exists, fair, atomic, serial, parallel, oneof, require, pass, self, global, return, if, elif, else, for, while, break, continue, return, enum, etc

16. `any` statements for nondeterminism
16a. `any` statements are used to introduce nondeterminism. The syntax is `x = any <iterable_variable>`. The model checker will explore all possible paths with each of the expressions in the list. The value can be used in the subsequent statements.
16b. `any` statements can also include conditional, like `y = any <iterable_variable> if y != x`. This will explore cases where y is different from x.
16c. If there are no elements in the `any` or the condition is False for all values of iterable, then the transition is disabled.

---
title: Enums, Records, and Collections
description: Learn how to use enums, records, collections, and other common data structures in FizzBee.
weight: -15
---

As with standard Python, FizzBee supports the standard sets, lists, and dictionaries.
In addition to the standard sets and dictionaries where Python requires the types to be hashable,
FizzBee also supports alternatives `genericset` and `genericmap` where the elements/keys can be
non-hashable types.


{{< toc >}}

## Enums

To define an `enum`, Usually, these are defined at the top level.

{{% fizzbee %}}
Color = enum('RED', 'YELLOW', 'GREEN')

action Init:
  color = Color.RED

action Next:
  if color == Color.RED:
    color = Color.GREEN
  elif color == Color.GREEN:
    color = Color.YELLOW
  else:
    color = Color.RED
{{% /fizzbee %}}

To choose iterate over the enum values in `for` or `any`, you can simply use `dir()` builtin.

{{% fizzbee %}}
Color = enum('RED', 'YELLOW', 'GREEN')

action Init:
  color = Color.RED

action Next:
  any c in dir(Color):
    color = c
{{% /fizzbee %}}

{{% hint %}}
`enum` in FizzBee is only a syntactic sugar over the standard string. So
`Color.RED == 'RED'` is true. It is not a separate type, like Python's `enum.Enum`.

{{% /hint %}}

## Records

To define a `record`, These can be defined anywhere a standard python statement can be defined.

```python
# Define a record
msg = record(node=5, color=Color.RED, message='Hello')

# Access the fields
msg.node
# Set the fields
msg.color = Color.GREEN
```

## Immutable Struct
This may not be that useful, this comes with the Starlark language (that FizzBee is actually based on) so if needed use it.

```python
msg = struct(node=5, color=Color.RED, message='Hello')

msg.node = 10 # This will throw an error
```
As an implementation detail, the `enum` feature is implemented as a struct with keys as the enum values.

## Collections
### List
Ordered list of elements. Common standard Python operations are supported.
`append,clear,extend,index,insert,pop,remove`
```python
list = [1, 2, 3]
list[0] = 4
```

### Set
Unordered collection of unique elements. Common standard Python operations are supported.
`add,clear,difference,discard,intersection,issubset,issuperset,pop,remove,symmetric_difference,union`

```python

s = set([1, 2, 2])
# len(s) == 2
s.add(4)
# len(s) == 3
s.remove(2)
# len(s) == 2
```

### Dictionary
Key-value pairs. Common standard Python operations are supported.
`clear,get,items,keys,pop,popitem,setdefault,update,values`

```python

d = {1: 'one', 2: 'two'}

d[1] = 'uno'
```

### Generic Set
Unordered collection of unique elements. The elements can be non-hashable.
The standard methods like
`add,clear,difference,discard,intersection,issubset,issuperset,pop,remove,symmetric_difference,union`
are supported, and looping over the elements is also supported

```python

s = genericset()
s.add({'status': 'working', 'id': 1})
s.add({'status': 'pending', 'id': 2})
# len(s) == 2

# Adding the same element again does not change the size
s.add({'status': 'working', 'id': 1})
# len(s) == 2
```

### Generic Map
Key-value pairs. The keys can be non-hashable.
The standard methods like `clear,get,items,keys,pop,popitem,setdefault,update,values`
are supported, and looping over the elements is also supported.

```python

d = genericmap()
d[{'status': 'working', 'id': 1}] = 'one'
d[{'status': 'pending', 'id': 2}] = 'two'
```

### Bag
A bag is a collection of elements where duplicates are allowed.
The standard methods like `add,add_all,clear,discard,pop,remove` and looping over the elements are supported.  Bag is also known as multiset.

```python

b = bag()
b.add(1)
b.add(2)
b.add(2)
# len(b) == 3

print(b == bag([2, 1, 2])) # True Because, order of elements does not matter
print(b == bag([2, 1])) # False, because the number of '2' elements are different
```

---
title: Guard Clauses And Enabling Conditions
description: Learn how guard clauses and enabling conditions work in FizzBee to control action transitions. This guide covers testing for deadlocks, liveness, and common pitfalls—a must-read before you start writing your first model.
weight: -20
---

Before you do any serious modeling in any formal system, you need to understand the concept of guard clauses and enabling conditions.

Guard clauses or enabling conditions are the predicates that tell whether an action/transition is allowed or not.
Guard clauses are critical to test deadlocks and liveness properties.

Some formal methods languages like Event-B, PRISM etc use standalone guard clauses or preconditions.
FizzBee follows languages like TLA+ use embedded guard clauses where the guard is part of the action.
But unlike TLA+, FizzBee infers the enabled status of the action using a simple but unconventional set of rules.


{{< toc >}}

## Example: Simple Light Switch

{{% fizzbee %}}
action Init:
  switch = "OFF"

atomic action On:
  if switch != "ON":
    switch = "ON"

{{% /fizzbee %}}

When you run the model checker, it will show a deadlock error. Because, once the switch is turned ON,
then no other action is possible.

Obviously, two correct ways to fix this, depending on the requirements or the expected behavior.
1. Remove the guard clause from the `On` action. This would make, the ON action idempotent.
   That's like a elevator call button. You can press it multiple times, but it will not change the state.
   {{% fizzbee %}}
action Init:
  switch = "OFF"

atomic action On:
    switch = "ON"

   {{% /fizzbee %}}

2. Allow other actions like `Off`, that will change the state back to OFF.

{{% fizzbee %}}
action Init:
  switch = "OFF"

atomic action On:
  if switch != "ON":
    switch = "ON"

atomic action Off:
  if switch != "OFF":
    switch = "OFF"

{{% /fizzbee %}}

Instead of using if-else, you can also use `require` statements.

{{% fizzbee %}}
action Init:
  switch = "OFF"

atomic action On:
  require switch != "ON"
  switch = "ON"

atomic action Off:
  require switch != "OFF"
  switch = "OFF"

{{% /fizzbee %}}

Although if-else structure and the require statement are similar, they have some subtle differences,
that we will go over later in this doc.

## Implementation detail
In an ideal world, you probably don't need to know this. But a brief understanding of the implementation details
will help you spec the model effectively and help simplify the code and
speed up model checking.

FizzBee has multiple types of statements, similar to Python.
One of the types is the `simple_python_stmt`, that corresponds to a subset of [Python's `simple_stmt` in this grammar](https://docs.python.org/3/reference/grammar.html).
Specifically,
- assignments (a = 1, x+=2 etc)
- star_expressions (my_list.append(1) etc)
- pass
- del (at present Python's `del` stmt is not supported, but will be added soon)
Other statements like `if-elif-else`, `for`, `any`, `while`, `return`, `continue` etc are not `simple_python_stmt`.

There is one exception for the method calls. If the method call is to builtin functions or functions in
the standard library, it is treated as `simple_python_stmt`.
If it is a user-defined function or a fizz function, even though it is a syntactically same as
the builtin function, it is not treated as `simple_python_stmt`. Let us call it, `fizz_call_stmt`.

In many rules, this exception will be a bit annoying to learn,
but you'll get it once you see a lot of examples.


In the atomic context, this is how the enabled status is inferred.

1. Every action starts out as disabled `action.enabled=False`.

2. For each statement that is executed, if the statement is a

   2a. `simple_python_stmt`, enable the action `action.enabled=True`.

   2b. `fizz_call_stmt` and the returned value is assigned, then enable the action `action.enabled=True`.

   2c. if `require` fails, disable the action `action.enabled=False`, and exits the action.

   2d. for any other statements, leave the enabled bit unchanged.

3. At the end of the action, if `action.enabled == True`, the action will be added to the state graph,
   and check for invariants etc.

The rules are simple and allows for an incredibly concise and expressive language.


{{% fizzbee %}}
action Init:
  switch = "OFF"

atomic action On:           # action.enabled = False
  if switch != "ON":        # UNCHANGED action.enabled irrespective of whether if is True or False
    switch = "ON"           # action.enabled = True

{{% /fizzbee %}}

If the switch is in OFF state, the ON action will be enabled only,
when the `switch = "ON"` gets executed.

If the switch is in ON state already, the action starts out disabled.
And since the `switch = "ON"` is not executed, the action will remain disabled.

That is, the above code is equivalent to the following code.
{{% fizzbee %}}
action Init:
  switch = "OFF"

atomic action On:           # action.enabled = False
  if switch == "ON":        # UNCHANGED action.enabled irrespective of whether if is True or False
    return                  # UNCHANGED action.enabled
  switch = "ON"             # action.enabled = True

{{% /fizzbee %}}

Similarly, when you have if,elif,else, the action will be enabled only if at least one simple statement is executed.

{{% fizzbee %}}
action Init:
  switch = "OFF"

atomic action On:           # action.enabled = False
  if switch == "ON":        # UNCHANGED action.enabled irrespective of whether if is True or False
    return                  # UNCHANGED action.enabled
  else:                     # UNCHANGED action.enabled
    switch = "ON"           # action.enabled = True

{{% /fizzbee %}}

Of course, if instead of `return` if you use any statement, it will enable the action.

{{% fizzbee %}}
action Init:
  switch = "OFF"

atomic action On:           # action.enabled = False
  if switch == "ON":        # UNCHANGED action.enabled irrespective of whether if is True or False
    x = 1                   # action.enabled = True
  else:                     # UNCHANGED action.enabled
    switch = "ON"           # action.enabled = True

{{% /fizzbee %}}

## A possible pitfall
Although the rule is simple, some seemingly equivalent python code will not work as expected.

For example: in this example, the On action will be enabled even if it is already ON.

{{% fizzbee %}}
action Init:
    switch = "OFF"

atomic action On:               # action.enabled = False
    is_on = (switch == "ON")    # action.enabled = True
    if is_on:                   # UNCHANGED action.enabled (i.e True)
        switch = "ON"           # action.enabled = True

{{% /fizzbee %}}

The reason is, the `is_on` is a `simple_python_stmt` and it enables the action when executed.

This is a bit counter-intuitive, but it is a design choice to keep the language simple and expressive.

So, one way to still disable the action is to use `require` statement

{{% fizzbee %}}
action Init:
    switch = "OFF"

atomic action On:               # action.enabled = False
    is_on = (switch == "ON")    # action.enabled = True
    require switch != "ON"      # action.enabled = False if switch is ON and aborts the action,
                                # otherwise, UNCHANGED action.enabled
    switch = "ON"               # action.enabled = True

{{% /fizzbee %}}

In this case, when the switch is already ON, when it executes the first statement,
the action will be tentatively enabled, but the `require` statement will disable it.
And since it aborts the action, the action remains disabled.

This equally applies to `for`, `any`, `while`, function calls etc. If within a `for` loop,
if there is at least one `simple_python_stmt` executed, the action will be enabled.

If the code calls a fizz function, and within that function there is a `simple_python_stmt` executed,
the action will be enabled.

---
title: Current Limitations
weight: 1000
---

FizzBee being a new system, that is continuously being improved up on.
* Building a python-like language is hard.
* Building a formal methods model checker is hard.
* Building a formal methods language that's python-like is even harder.

But we do these, because we are frustrated by the current state of formal methods. That said,
FizzBee is a work in progress. There are some gotchas and limitations, some are
temporary, some may not be addressed for a while.

I would list some of the common issues with the language implementation if you
expect it to be as good as Python itself. If you face any other issues, please raise
as issue on the [github page](https://github.com/fizzbee-io/fizzbee)

## Line numbers in error messages
Currently, the error messages in line numbers are obviously wrong. This will be worked
on next as a high priority.

## Fizz functions can be called only in a limited ways
Currently, fizz functions can be called only in the following ways:
```
[variable_name =] [role.]fizz_function_name([parameters])
```
That is,
```python
# top level functions
fizz_function_name(arg1, arg2) # zero or more args
# with return value
state = fizz_function_name(arg1, arg2 )

# functions within a role
replica.fizz_function_name(arg1, arg2)
state = replica.fizz_function_name(arg1, arg2 )

```
For all other cases, fizz treats the entire expression as a regular python code,
and runs using starlark interpreter, that will not recognize the fizz functions.
So you will get an error like,
```
Error evaluating expr: filename.fizz:1:1: undefined: fizz_function_name
```
If you get this error, you might have to extract the local variables to a separate
statement. For example,

```python
# This will not work
if replica.fizz_function_name(arg1, arg2) == 1:
    pass

# This will work
state = replica.fizz_function_name(arg1, arg2)
if state == 1:
    pass

# This will not work.
replica[i].fizz_function_name(arg1, arg2)
# This will work
r = replica[i]
r.fizz_function_name(arg1, arg2)

```
That said, standard python functions like max, len etc will work as expected.
This limit applies only to the fizz functions.

## Extracting local variables in a separate statement might alter the behavior

This will be the most confusing part of the language design. Being an abstract model
that simulates data storage with in-memory variables, a bit of care is needed when
extracting local variables to a separate statement.

For example, consider the following code:
```python
action Init:
  var1 = 0
  var2 = 0

always assertion BothNever1:
  return var1 == 0 or var2 == 0

action SetVar1:
  if var2 == 0:
    var1 = 1

action SetVar2:
  if var1 == 0:
    var2 = 1
```

vs

```python
action Init:
  var1 = 0
  var2 = 0

always assertion BothNever1:
  return var1 == 0 or var2 == 0

action SetVar1:
  if var2 == 0:
    var1 = 1

action SetVar2:
  local = var1
  if local == 0:
    var2 = 1
```

In the first case, the assertion will always pass. But in the second case, the assertion
will fail. This is because, imagining var1 and var2 are coming from a database.
When you extract the local variable, you are actually taking a snapshot of the database
field. And them there could be some context switching that could change the value of var1.




---
title: Whiteboard Visualizations
weight: 30
description: "Automatically generate sequence diagrams, state diagrams, and algorithm animations from simple pseudo code. Explore every state and transition interactively, ensuring your algorithms meet all requirements with zero extra effort."

---

FizzBee generates multiple visualizations and diagrams. So far you would have seen,
automatically generated state diagrams. In addition to state diagrams, FizzBee also
generates sequence diagrams and algorithm visualizations.

These are helpful in understanding the algorithm and the system behavior.


{{% toc %}}

| **Feature**                  | **Description**                                                                                                                                              |
|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Pseudo Code to Visualizations** | Generates sequence, state, and algorithm diagrams directly from high-level, Python-like pseudo code.                                                                 |
| **Zero Extra Effort**        | Visualizations are created directly from your pseudo code with no additional manual input required.                                                          |
| **Interactive Experience**   | Provides a dynamic, whiteboard-like interface to explore states, transitions, and scenarios in real time.                                                    |
| **Comprehensive Simulation** | Automatically simulates complex scenarios like non-atomic operations, crashes, message loss, concurrency, and visualizes all possible states.                |
| **Correctness Assertion**    | Enables adding assertions to ensure the design meets safety and liveness requirements.                                                                       |
| **Easy to Use**              | Familiar Python-like syntax reduces the learning curve compared to other formal methods tools.                                                               |

## Enable whiteboard visualizations
On the fizzbee playground, you can enable the whiteboard visualizations by setting
the `Enable Whiteboard` checkbox. Then click `Run` button.

Once the model checking is done, you can see a link to the `Explorer` in the `Console`.

Clicking on the `Explorer` link will open the explorer in a new tab.

## Example 1: Two phase commit
Before we go over the tutorial, let's see a complete working example.

1. Open the [Two Phase Commit](https://fizzbee.io/design/examples/two_phase_commit_actors/#complete-code) example
in the play ground

{{< expand "Show fizzbee spec for Two Phase Commit" >}}
For the detailed explanation of the spec, see [Two Phase Commit](https://fizzbee.io/design/examples/two_phase_commit_actors/)
Click 'Run in playground'

{{< fizzbee >}}

NUM_PARTICIPANTS = 2

role Coordinator:

  action Init:
    self.prepared = set()
    self.state = "init"

  action Write:
    if self.state != "init":
      return
    self.state = "working"
    for rm in self.PARTICIPANTS:
      vote = rm.Prepare()

      if vote == 'aborted':
        self.Abort()
        return

      self.prepared.add(rm.ID)

    self.Commit()


  fair action Timeout:
    if self.state != "committed":
      self.Abort()

  fair action Restart:
    if self.state == "committed":
      for rm in self.PARTICIPANTS:
        rm.Commit()

  func Abort():
      self.state = "aborted"
      for rm in self.PARTICIPANTS:
          rm.Abort()

  func Commit():
    if self.state == 'working' and len(self.prepared) == len(self.PARTICIPANTS):
      self.state = 'committed'
      for rm in self.PARTICIPANTS:
        rm.Commit()


role Participant:
  action Init:
    self.state = "working"

  fair action Timeout:
    if self.state == "working":
      self.state = "aborted"

  func Prepare():
    if self.state != 'working':
      return self.state
    oneof:
      self.state = 'prepared'
      self.state = 'aborted'
    return self.state

  func Commit():
    self.state = 'committed'

  func Abort():
    self.state = 'aborted'


always assertion ResMgrsConsistent:
  for rm1 in participants:
    for rm2 in participants:
      if rm1.state == 'committed' and rm2.state == 'aborted':
        return False
  return True

eventually always assertion Terminated:
  return coordinator.state in ('committed', 'aborted')


action Init:
  participants = []
  for i in range(NUM_PARTICIPANTS):
    p = Participant(ID=i)
    participants.append(p)

  coordinator = Coordinator(PARTICIPANTS=participants)

{{< /fizzbee >}}

{{< /expand >}}

2. Enable `Enable Whiteboard` checkbox, and Run.
3. Once the model checker completes, click on `Communication Diagram` link in the console.
   This will show the block diagram for the two phase commit protocol.
    {{% graphviz %}}
digraph G {
  node [shape=box];
  splines=false;
  rankdir=LR;
  "Participant" [shape=none label=<<table cellpadding="14" cellspacing="8" style="dashed">
      <tr><td port="p0">Participant#0</td></tr>
      <tr><td port="p1" border="0">&#x022EE;</td></tr>
      <tr><td port="p2">Participant#2</td></tr>
      </table>>]
  "Coordinator" -> "Participant":p0 [label="Prepare, Abort, Commit"];
  "Coordinator" -> "Participant":p2 [label="Prepare, Abort, Commit"];
  "FairActionParticipant" [shape=none label=<<table cellpadding="14" cellspacing="8" style="invisible"><tr>
      <td port="Timeout"></td>
      </tr></table>>]
  { rank=same; "Participant"; "FairActionParticipant"; }
  "FairActionParticipant":Timeout -> "Participant" [label="Timeout"];
  "actionCoordinatorWrite" [label="" shape="none"]
  "actionCoordinatorWrite" -> "Coordinator" [label="Write"];
  "FairActionCoordinator" [shape=none label=<<table cellpadding="14" cellspacing="8" style="invisible"><tr>
      <td port="Timeout"></td>
      <td port="Restart"></td>
      </tr></table>>]
  { rank=same; "Coordinator"; "FairActionCoordinator"; }
  "FairActionCoordinator":Timeout -> "Coordinator" [label="Timeout"];
  "FairActionCoordinator":Restart -> "Coordinator" [label="Restart"];
}
    {{% /graphviz %}}
4. Click on the `Explorer` link in the console.
5. You would see the Init state of the system
   {{% graphviz %}}
   digraph G {
   compound=true;
   subgraph "cluster_Participant#0" {
   style=dashed;
   label="Participant#0";
   "Participant#0_placeholder" [label="" shape=point width=0 height=0 style=invis];
   "Participant#0.state" [label="state = working" shape=ellipse];
   }
   subgraph "cluster_Participant#1" {
   style=dashed;
   label="Participant#1";
   "Participant#1_placeholder" [label="" shape=point width=0 height=0 style=invis];
   "Participant#1.state" [label="state = working" shape=ellipse];
   }
   subgraph "cluster_Coordinator#0" {
   style=dashed;
   label="Coordinator#0";
   "Coordinator#0_placeholder" [label="" shape=point width=0 height=0 style=invis];
   subgraph "cluster_Coordinator#0.prepared" {
   style=dashed;
   label="prepared";
   node [shape=record, style=filled, fillcolor=white];
   "Coordinator#0.prepared" [label="{  }"];
   }
   "Coordinator#0.state" [label="state = init" shape=ellipse];
   }
   "coordinator" [label="coordinator = role Coordinator#0" shape=ellipse];
   "coordinator" -> "Coordinator#0_placeholder" [lhead="cluster_Coordinator#0"];
   subgraph "cluster_null.participants" {
   style=dashed;
   label="participants";
   "participants" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td port="before" sides="b"></td></tr>
<tr><td port="0">role Participant#0</td></tr>
<tr><td port="1">role Participant#1</td></tr>
<tr><td port="after" sides="t"></td>
</tr></table>>];
}
"participants":0 -> "Participant#0_placeholder" [lhead="cluster_Participant#0"];
"participants":1 -> "Participant#1_placeholder" [lhead="cluster_Participant#1"];
}
   {{% /graphviz %}}
5. On the left, you will see various buttons for the actions. Click on the `Coordinator#0.Write` button.
   it should start the sequence diagram. Continue to click on the buttons to see the sequence diagram.
   If you always choose the success path, you will see eventually both the participants and the coordinator
   reach the `committed` state.
   {{< mermaid class="text-center" >}}
sequenceDiagram
	note left of 'Coordinator#0': Write
	'Coordinator#0' ->> 'Participant#0': Prepare()
	'Participant#0' -->> 'Coordinator#0': ("prepared")
	'Coordinator#0' ->> 'Participant#1': Prepare()
	'Participant#1' -->> 'Coordinator#0': ("prepared")
	'Coordinator#0' ->> 'Participant#0': Commit()
	'Participant#0' -->> 'Coordinator#0': .
	'Coordinator#0' ->> 'Participant#1': Commit()
	'Participant#1' -->> 'Coordinator#0': .
   {{< /mermaid >}}

   {{% graphviz %}}
digraph G {
compound=true;
subgraph "cluster_Participant#0" {
style=dashed;
label="Participant#0";
"Participant#0_placeholder" [label="" shape=point width=0 height=0 style=invis];
"Participant#0.state" [label="state = committed" shape=ellipse];
}
subgraph "cluster_Participant#1" {
style=dashed;
label="Participant#1";
"Participant#1_placeholder" [label="" shape=point width=0 height=0 style=invis];
"Participant#1.state" [label="state = committed" shape=ellipse];
}
subgraph "cluster_Coordinator#0" {
style=dashed;
label="Coordinator#0";
"Coordinator#0_placeholder" [label="" shape=point width=0 height=0 style=invis];
subgraph "cluster_Coordinator#0.prepared" {
style=dashed;
label="prepared";
"Coordinator#0.prepared" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td port="before" sides="r"></td>
<td port="0">0</td>
<td port="1">1</td>
<td port="after" sides="l"></td>
</tr></table>>];
}

"Coordinator#0.state" [label="state = committed" shape=ellipse];
}
"coordinator" [label="coordinator = role Coordinator#0" shape=ellipse];
"coordinator" -> "Coordinator#0_placeholder" [lhead="cluster_Coordinator#0"];
subgraph "cluster_null.participants" {
style=dashed;
label="participants";
"participants" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td port="before" sides="b"></td></tr>
<tr><td port="0">role Participant#0</td></tr>
<tr><td port="1">role Participant#1</td></tr>
<tr><td port="after" sides="t"></td>
</tr></table>>];
}
"participants":0 -> "Participant#0_placeholder" [lhead="cluster_Participant#0"];
"participants":1 -> "Participant#1_placeholder" [lhead="cluster_Participant#1"];
}
   {{% /graphviz %}}

6. Play around with the explorer to see what happens when a participant aborted.
   {{< mermaid class="text-center" >}}
   sequenceDiagram
   note left of 'Coordinator#0': Write
   'Coordinator#0' ->> 'Participant#0': Prepare()
   'Participant#0' -->> 'Coordinator#0': ("prepared")
   'Coordinator#0' ->> 'Participant#1': Prepare()
   'Participant#1' -->> 'Coordinator#0': ("aborted")
   'Coordinator#0' ->> 'Participant#0': Abort()
   'Participant#0' -->> 'Coordinator#0': .
   'Coordinator#0' ->> 'Participant#1': Abort()
   'Participant#1' -->> 'Coordinator#0': .
   {{< /mermaid >}}

   {{% graphviz %}}
   digraph G {
   compound=true;
   subgraph "cluster_Participant#0" {
   style=dashed;
   label="Participant#0";
   "Participant#0_placeholder" [label="" shape=point width=0 height=0 style=invis];
   "Participant#0.state" [label="state = aborted" shape=ellipse];
   }
   subgraph "cluster_Participant#1" {
   style=dashed;
   label="Participant#1";
   "Participant#1_placeholder" [label="" shape=point width=0 height=0 style=invis];
   "Participant#1.state" [label="state = aborted" shape=ellipse];
   }
   subgraph "cluster_Coordinator#0" {
   style=dashed;
   label="Coordinator#0";
   "Coordinator#0_placeholder" [label="" shape=point width=0 height=0 style=invis];
   subgraph "cluster_Coordinator#0.prepared" {
   style=dashed;
   label="prepared";
   "Coordinator#0.prepared" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td port="before" sides="r"></td>
<td port="0">0</td>
<td port="after" sides="l"></td>
</tr></table>>];
}

"Coordinator#0.state" [label="state = aborted" shape=ellipse];
}
"coordinator" [label="coordinator = role Coordinator#0" shape=ellipse];
"coordinator" -> "Coordinator#0_placeholder" [lhead="cluster_Coordinator#0"];
subgraph "cluster_null.participants" {
style=dashed;
label="participants";
"participants" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td port="before" sides="b"></td></tr>
<tr><td port="0">role Participant#0</td></tr>
<tr><td port="1">role Participant#1</td></tr>
<tr><td port="after" sides="t"></td>
</tr></table>>];
}
"participants":0 -> "Participant#0_placeholder" [lhead="cluster_Participant#0"];
"participants":1 -> "Participant#1_placeholder" [lhead="cluster_Participant#1"];
}
   {{% /graphviz %}}
7. You can explorer most possible sequences and states.


## Example 2: Insertion Sort
Let's try another visualization - the Insertion Sort algorithm. FizzBee is a formal
verification language, so verifying Insertion Sort is not the best use case. But this introduces
the visualization capabilities.

If you look at the insertion_sort method, the code is actually a working python code
(with only the method definition syntax being different)

### Checkpoint

Checkpoint is similar to breakpoint in debugger, but this is not running the code during exploration
instead it executes the model and checkpoints the state at various points.

There are default checkpoints at the start of each action, any non-determinism
or when there are any yields.

You can set explicit checkpoints using `` `checkpoint` ``.

### FizzBee spec

{{% fizzbee %}}
---
deadlock_detection: false
options:
       max_actions: 1
---

atomic action InsertionSort:
    insertion_sort(arr)

atomic func insertion_sort(arr):
	if len(arr) <= 1:
		return # If the array has 0 or 1 element, it is already sorted, so return

	for i in range(1, len(arr)): # Iterate over the array starting from the second element
        cur = i
        ins = i-1

		key = arr[i] # Store the current element as the key to be inserted in the right position

        j = i-1
        `checkpoint`
    	while j >= 0 and key < arr[j]: # Move elements greater than key one position ahead
                ins = j
    			arr[j+1] = arr[j] # Shift elements to the right

                `checkpoint`
    			j -= 1

		arr[j+1] = key # Insert the key in the correct position

action Init:
    # Sorting the array [12, 11, 13, 5, 6] using insertionSort
    # arr = [5, 3, 2, 4, 1]
    arr = [12, 11, 13, 5, 6]
    # arr = ["a12", "a11", "a13", "a5", "a6", "a1"]
    cur = len(arr)
    ins = -1
    key = arr[0]


{{% /fizzbee %}}

1. Run the above code in the playground with `Enable Whiteboard` checked.
2. Click on the `Explorer` link in the console
3. Play around by clicking the buttons. It will only show the states, there are no
   sequence diagrams for this example. (Sequence diagrams are only generated when there are roles)
4. While the states are shown, with `cur` and `ins` variable, it would be nicer
   to show them as pointers to the position in the array. This information is not
   available in the spec so far. So this should be added as an additional config.
   {{% graphviz %}}
   digraph G {
   compound=true;
   subgraph "cluster_null.arr" {
   style=dashed;
   label="arr";
   "arr" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td port="before" sides="r"></td>
<td port="0">12</td>
<td port="1">11</td>
<td port="2">13</td>
<td port="3">5</td>
<td port="4">6</td>
<td port="after" sides="l"></td>
</tr></table>>];
}

"cur" [label="cur = 1" shape=ellipse];
"ins" [label="ins = 0" shape=ellipse];
"key" [label="key = 11" shape=ellipse];
}
   {{% /graphviz %}}

5. On the left-bottom, you will see the `Config` text area. Insert the following code.
   ```yaml
   variables:
     cur:
       index_of: arr
     ins:
       index_of: arr
   ```
   {{% graphviz %}}
   digraph G {
   compound=true;
   subgraph "cluster_null.arr" {
   style=dashed;
   label="arr";
   "arr" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td port="before" sides="r"></td>
<td port="0">12</td>
<td port="1">11</td>
<td port="2">13</td>
<td port="3">5</td>
<td port="4">6</td>
<td port="after" sides="l"></td>
</tr></table>>];
}

"cur" [label="cur = 1" shape=ellipse];
"ins" [label="ins = 0" shape=ellipse];
"key" [label="key = 11" shape=ellipse];
"cur" -> "arr":1;
"ins" -> "arr":0;
}
   {{% /graphviz %}}

6. Click on buttons to see the states change as the algorithm progresses.

# Basic Usage

FizzBee is not a usual visualization tool, it is a design specification system.
You can model your design in an python-like language and use that to validate the design
to check for issues like concurrency, consistency, fault tolerance, then model performance
like error ratio, tail latency distribution and more.

As a design specification, FizzBee generates various visualizations to help you understand
the system behavior that can be shard with your team. This makes it easier to explain the design
for your team to review, saving significant amount of time.

For more details on the language, refer to the [FizzBee Language Reference](https://fizzbee.io/design/tutorials/getting-started/).

In the document, we will do a quick overview of the language necessary to generate the visualizations
and skip the model checking parts.

## Python datatypes
FizzBee uses a variant of Python syntax, and for the most part relies on Starlark for evaluating expressions.
So all the standard Python datatypes like `int`, `float`, `str`, `list`, `dict` etc are supported.

{{% fizzbee %}}
---
deadlock_detection: false
---

action Init:
    elements = [4, 2, 1, 5, 3]
    cursor = 3
    matrix = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    obj = {"store_id": 1001, "city": "San Francisco", "sqft": 12000.5}
    ordered = [ ("store_id", 1001), ("city", "San Francisco"), ("sqft", 12000.5)]
    obj_array = [{"name": "Alice", "color": "blue", "score": 10}, {"name": "Bob", "color": "green", "score": 9.5, "lang": ["English", "Spanish"]}]

{{% /fizzbee %}}

{{% hint %}}
The top section is the frontmatter, where you can set various model checking options.

In this example, our spec just initializes the variables, and there are no action after that.
So effectively, this implies the system cannot progress after the Init, so it is considered a deadlock
For this example, we are just suppressing this error.
{{% /hint %}}

Run this example with the whiteboard enabled, then click on the `Explorer` link in the console.

You should see something like,
{{% graphviz %}}
digraph G {
compound=true;
target="_blank";
"cursor" [label="cursor = 3" shape=ellipse];
subgraph "cluster_null.elements" {
style=dashed;
label="elements";
"elements" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td port="before" sides="r"></td>
<td port="0">4</td>
<td port="1">2</td>
<td port="2">1</td>
<td port="3">5</td>
<td port="4">3</td>
<td port="after" sides="l"></td>
</tr></table>>];
}

subgraph "cluster_null.matrix" {
style=dashed;
label="matrix";
"matrix" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td colspan="100" port="before" sides="b" cellpadding="0"></td></tr>
<tr><td port="0">1</td>
<td port="0">2</td>
<td port="0">3</td></tr>
<tr><td port="1">4</td>
<td port="1">5</td>
<td port="1">6</td></tr>
<tr><td port="2">7</td>
<td port="2">8</td>
<td port="2">9</td></tr>
<tr><td colspan="100" port="after"  sides="t" cellpadding="0"></td>
</tr></table>>];
}

subgraph "cluster_null.obj" {
style=dashed;
label="obj";
"obj" [shape=plaintext, label=<
<table border="0" cellborder="1" cellspacing="0" cellpadding="4">
  <tr>
    <td port="keycity">city</td>
    <td port="valuecity" >'San Francisco'</td>
  </tr>
  <tr>
    <td port="keysqft">sqft</td>
    <td port="valuesqft" >12000.5</td>
  </tr>
  <tr>
    <td port="keystore_id">store_id</td>
    <td port="valuestore_id" >1001</td>
  </tr>
</table>
>];
}
subgraph "cluster_null.obj_array" {
style=dashed;
label="obj_array";
"obj_array" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td><b>name</b></td>
<td><b>color</b></td>
<td><b>score</b></td>
<td><b>lang</b></td></tr>
<tr><td colspan="100" port="before" sides="b" cellpadding="0"></td></tr>
<tr><td port="0">Alice</td>
<td port="0">blue</td>
<td port="0">10</td>
<td port="0"></td></tr>
<tr><td port="1">Bob</td>
<td port="1">green</td>
<td port="1">9.5</td>
<td port="1">['English', 'Spanish']</td></tr>
<tr><td colspan="100" port="after"  sides="t" cellpadding="0"></td>
</tr></table>>];
}

subgraph "cluster_null.ordered" {
style=dashed;
label="ordered";
"ordered" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td colspan="100" port="before" sides="b" cellpadding="0"></td></tr>
<tr><td port="0">store_id</td>
<td port="0">1001</td></tr>
<tr><td port="1">city</td>
<td port="1">San Francisco</td></tr>
<tr><td port="2">sqft</td>
<td port="2">12000.5</td></tr>
<tr><td colspan="100" port="after"  sides="t" cellpadding="0"></td>
</tr></table>>];
}

}
{{% /graphviz %}}

## Simple actions
Actions are the basic building blocks of the system. They can either represent steps triggered
by the user or the components that are external to the system being modelled.

{{% fizzbee %}}
action Init:
    elements = []

action Produce:
    require len(elements) < 4
    x = any range(4)
    elements.append(x)

action Consume:
    require len(elements) > 0
    elements.pop(0)

{{% /fizzbee %}}

Open the playground, and try the whiteboard. This time you will see, the `elements` list
that is initially empty. On the left, click on the `Produce` button,
then pick an element to add to the list.
you will see the `elements` getting updated. Once an element is added, you would see
two actions `Produce` and `Consume` available (as shown in the image below). Click on `Consume` to remove the element.

{{% rawhtml %}}
<div class="buttons">
    <button id="undo">Undo</button>
    <div id="link-buttons"><button style="margin: 5px 10px 5px 0px;">Produce</button><button style="margin: 5px 10px 5px 0px;">Consume</button></div>
  </div>
{{% /rawhtml %}}
{{% graphviz %}}
digraph G {
compound=true;
target="_blank";
subgraph "cluster_null.elements" {
style=dashed;
label="elements";
"elements" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td port="before" sides="r"></td>
<td port="0">2</td>
<td port="1">1</td>
<td port="2">3</td>
<td port="after" sides="l"></td>
</tr></table>>];
}

}
{{% /graphviz %}}

Try Produce multiple times to fill to capacity, then you will see Produce action is not enabled anymore.
This should explain how `require` and `any` keywords work

## Array index
We saw how to display the datastructure. Many times, we would want to see
how the data access. Since the index information is not preserved, we need to
add a bit of extra configuration.

{{% fizzbee %}}

action Init:
    elements = [1, 2, 3, 4, 5]
    cursor = -1
    value = None

atomic action MoveCursor:
    if cursor > len(elements):
        cursor = -1
        value = None
    elif cursor < len(elements) - 1:
        cursor += 1
        value = elements[cursor]
    else:
        cursor += 1
        value = None

{{% /fizzbee %}}

I will show the block diagram like this.
{{% graphviz %}}
digraph G {
compound=true;
target="_blank";
"cursor" [label="cursor = -1" shape=ellipse];
subgraph "cluster_null.elements" {
style=dashed;
label="elements";
"elements" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td port="before" sides="r"></td>
<td port="0">1</td>
<td port="1">2</td>
<td port="2">3</td>
<td port="3">4</td>
<td port="4">5</td>
<td port="after" sides="l"></td>
</tr></table>>];
}

"value" [label="value = null" shape=ellipse];
}
{{% /graphviz %}}

Now, to specify the cursor variable points to the index of the elements array,

On the left bottom text area, set
```yaml
variables:
  cursor:
    index_of: elements
```
Now, click the MoveCursor buttons. You will see a diagram like this.

{{% graphviz %}}
digraph G {
compound=true;
target="_blank";
"cursor" [label="cursor = 2" shape=ellipse];
subgraph "cluster_null.elements" {
style=dashed;
label="elements";
"elements" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td port="before" sides="r"></td>
<td port="0">1</td>
<td port="1">2</td>
<td port="2">3</td>
<td port="3">4</td>
<td port="4">5</td>
<td port="after" sides="l"></td>
</tr></table>>];
}

"value" [label="value = 3" shape=ellipse];
"elements":2 -> "cursor" [dir=back] ;
}
{{% /graphviz %}}

## Checkpoints
In the above example, we saw how each action is a step in the system. Let us try slightly
more complex example where we have a loop.
This is a simple example of summing the elements in the array.
{{% fizzbee %}}

action Init:
    elements = [1, 2, 3, 4, 5]
    cursor = -1
    sum = 0

atomic action Sum:
    require sum == 0
    for i in range(0, len(elements)):
        cursor = i
        sum += elements[i]

atomic action Restart:
    require sum != 0
    cursor = -1
    sum = 0

{{% /fizzbee %}}

One action `Sum` sums the results, and the next action `Restart` resets the sum and cursor.

If you run this, you will see the sum being calculated. But you will not see the intermediate steps.

The states graph will show,
{{% graphviz %}}
digraph G {
"0x14000121c20" [label="yield
Actions: 0, Forks: 0
State: {\"cursor\":\"-1\",\"elements\":\"[1, 2, 3, 4, 5]\",\"sum\":\"0\"}
", color="black" penwidth="2" ];
"0x14000121c20" -> "0x14000121f20" [label="Sum", color="black" penwidth="1" ];
"0x14000121f20" [label="yield
Actions: 1, Forks: 1
State: {\"cursor\":\"4\",\"elements\":\"[1, 2, 3, 4, 5]\",\"sum\":\"15\"}
", color="black" penwidth="2" ];
"0x14000121f20" -> "0x14000121c20" [label="Restart", color="black" penwidth="1" ];
}
{{% /graphviz %}}

To see the intermediate steps, we can add a checkpoint in the loop.
{{% highlight udiff %}}
*** 7,12 ****
--- 7,13 ----
  atomic action Sum:
      require sum == 0
      for i in range(0, len(elements)):
+         `checkpoint`
          cursor = i
          sum += elements[i]
{{% /highlight %}}

Run and open the states graph, you will see a lot more intermediate steps.

{{% graphviz %}}
digraph G {
  "0x14000095c80" [label="yield
Actions: 0, Forks: 0
State: {\"cursor\":\"-1\",\"elements\":\"[1, 2, 3, 4, 5]\",\"sum\":\"0\"}
", color="black" penwidth="2" ];
  "0x14000095c80" -> "0x140001aa000" [label="Sum", color="black" penwidth="1" ];
  "0x140001aa000" [label="Sum
Actions: 1, Forks: 1
State: {\"cursor\":\"-1\",\"elements\":\"[1, 2, 3, 4, 5]\",\"sum\":\"0\"}
Threads: 0/1
", color="black" penwidth="1" ];
  "0x140001aa000" -> "0x140001aa600" [label="checkpoint[Sum.checkpoint]", color="black" penwidth="1" ];
  "0x140001aa600" [label="checkpoint
Actions: 1, Forks: 2
State: {\"cursor\":\"0\",\"elements\":\"[1, 2, 3, 4, 5]\",\"sum\":\"1\"}
Threads: 0/1
", color="black" penwidth="1" ];
  "0x140001aa600" -> "0x140001aad20" [label="checkpoint[Sum.checkpoint]", color="black" penwidth="1" ];
  "0x140001aad20" [label="checkpoint
Actions: 1, Forks: 3
State: {\"cursor\":\"1\",\"elements\":\"[1, 2, 3, 4, 5]\",\"sum\":\"3\"}
Threads: 0/1
", color="black" penwidth="1" ];
  "0x140001aad20" -> "0x140001ab200" [label="checkpoint[Sum.checkpoint]", color="black" penwidth="1" ];
  "0x140001ab200" [label="checkpoint
Actions: 1, Forks: 4
State: {\"cursor\":\"2\",\"elements\":\"[1, 2, 3, 4, 5]\",\"sum\":\"6\"}
Threads: 0/1
", color="black" penwidth="1" ];
  "0x140001ab200" -> "0x140001ab6e0" [label="checkpoint[Sum.checkpoint]", color="black" penwidth="1" ];
  "0x140001ab6e0" [label="checkpoint
Actions: 1, Forks: 5
State: {\"cursor\":\"3\",\"elements\":\"[1, 2, 3, 4, 5]\",\"sum\":\"10\"}
Threads: 0/1
", color="black" penwidth="1" ];
  "0x140001ab6e0" -> "0x140001abbc0" [label="checkpoint[Sum.checkpoint]", color="black" penwidth="1" ];
  "0x140001abbc0" [label="yield
Actions: 1, Forks: 6
State: {\"cursor\":\"4\",\"elements\":\"[1, 2, 3, 4, 5]\",\"sum\":\"15\"}
", color="black" penwidth="2" ];
  "0x140001abbc0" -> "0x14000095c80" [label="Restart", color="black" penwidth="1" ];
}
{{% /graphviz %}}

Then, open the explorer. Similar to previous example, set the cursor.

```yaml
variables:
  cursor:
    index_of: elements
```
Now, click the Sum button and play.
{{% graphviz %}}
digraph G {
compound=true;
target="_blank";
"cursor" [label="cursor = -1" shape=ellipse];
subgraph "cluster_null.elements" {
style=dashed;
label="elements";
"elements" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td port="before" sides="r"></td>
<td port="0">1</td>
<td port="1">2</td>
<td port="2">3</td>
<td port="3">4</td>
<td port="4">5</td>
<td port="after" sides="l"></td>
</tr></table>>];
}

"sum" [label="sum = 0" shape=ellipse];
"elements":before -> "cursor" [dir=back] ;
}
{{% /graphviz %}}

## Roles (Required for Sequence Diagrams)
To role more about roles, see [Roles](https://fizzbee.io/design/tutorials/roles)

Let us take a simple message passing example.

{{% fizzbee %}}
role Sender:
    action Process:
        receiver.Greet("Alice")
        pass

role Receiver:
    func Greet(name):
        return "Hello " + name

action Init:
    sender = Sender()
    receiver = Receiver()

{{% /fizzbee %}}

Once you run with the whiteboard enabled, open the explorer, and click `Process` button, you will see the sequence diagram.

{{< mermaid class="text-center" >}}
sequenceDiagram
	note left of 'Sender#0': Process
	'Sender#0' ->> 'Receiver#0': Greet(name: "Alice")
	'Receiver#0' -->> 'Sender#0': ("Hello Alice")
{{< /mermaid >}}

As a trivial example, there is only one action and none of these have any states.
Since there are no states, the system does not explore what happens if receiver is not available or if the message is lost,
or the receiver processed it, but the response was lost and so on.

Example: State updates with message passing

{{% fizzbee %}}
---
deadlock_detection: false
---
role Caller:
  action Init:
    self.state = 'init'

  action Send:
    require self.state == 'init'

    self.state = 'calling'
    res = s.Process()
    self.state = 'done'

role Server:
  action Init:
    self.state = 'init'

  func Process():
    self.state = 'done'

action Init:
  r = Caller()
  s = Server()

{{% /fizzbee %}}

When you run it, and open the explorer, you will see the states of the caller and server.
Obviously,
1. Both the caller and server are in the `init` state.
2. When you click on the `Send` button, the caller changes to `calling`, and server changes to `init` state.
3. At the point, wo possible things can happen.
   i. Happy case: Continue by clicking `thread-0`. The server changes to `done` state. You'll also see a sequence diagram.
   ii. Error case: Error case: Click on `crash`. This path implies, either the caller crashed before sending or server crashed before the server processed the request. So the caller will continue to think it is in calling state.
4. You can similarly try crash after the server processed the request, and see the server in `done` state and caller in `calling` state.
5. Finally, you can try crash after the caller received the response, and see the caller in `done` state and server in `done` state.

An example state where the caller is in `calling` state and server is in `done` state is shown below.


{{< mermaid class="text-center" >}}
sequenceDiagram
	note left of 'Caller#0': Send
	'Caller#0' ->> 'Server#0': Process()
	'Server#0' -->> 'Caller#0': .
	'Caller#0'->>'Caller#0': crash
{{< /mermaid >}}

{{% graphviz %}}
digraph G {
compound=true;
target="_blank";
subgraph "cluster_Caller#0" {
style=dashed;
label="Caller#0";
"Caller#0_placeholder" [label="" shape=point width=0 height=0 style=invis];
"Caller#0.state" [label="state = calling" shape=ellipse];
}
subgraph "cluster_Server#0" {
style=dashed;
label="Server#0";
"Server#0_placeholder" [label="" shape=point width=0 height=0 style=invis];
"Server#0.state" [label="state = done" shape=ellipse];
}
"r" [label="r = role Caller#0" shape=ellipse];
"r" -> "Caller#0_placeholder" [lhead="cluster_Caller#0"];
"s" [label="s = role Server#0" shape=ellipse];
"s" -> "Server#0_placeholder" [lhead="cluster_Server#0"];
}
{{% /graphviz %}}

Now, go back to the first example with two-phase commit and try to understand the behavior.

# Conclusion
FizzBee is a powerful tool to model and visualize the system behavior. In addition to checking for
correctness, it can also be used to generate visualizations that can be shared with the team to explain the design.

The best of all, unlike any other formal verification system,
FizzBee uses python'ish language making it easy to ramp up on day 1.

In other visualization tools, you will have to manually create the sequences. Most likely, you won't
be able to create the sequences for all the important paths. But with FizzBee, you can explore all possible paths.
Also, by describing your system design in a precise language, it doubles as a system specification language
replacing significant portions of the design document.









Example: Two phase commit
```
NUM_PARTICIPANTS = 2

role Coordinator:

  action Init:
    self.prepared = set()
    self.state = "init"

  action Write:
    if self.state != "init":
      return
    self.state = "working"
    for p in participants:
      vote = p.Prepare()

      if vote == 'aborted':
        self.Abort()
        return

      self.prepared.add(p.ID)

    self.Commit()


  fair action Timeout:
    if self.state != "committed":
      self.Abort()

  fair action Restart:
    if self.state == "committed":
      for p in participants:
        p.Commit()

  func Abort():
      self.state = "aborted"
      for p in participants:
          p.Abort()

  func Commit():
    if self.state == 'working' and len(self.prepared) == len(participants):
      self.state = 'committed'
      for p in participants:
        p.Commit()


role Participant:
  action Init:
    self.state = "working"

  fair action Timeout:
    if self.state == "working":
      self.state = "aborted"

  func Prepare():
    if self.state != 'working':
      return self.state
    oneof:
      self.state = 'prepared'
      self.state = 'aborted'
    return self.state

  func Commit():
    self.state = 'committed'

  func Abort():
    self.state = 'aborted'

always assertion ResMgrsConsistent:
  for p1 in participants:
    for p2 in participants:
      if p1.state == 'committed' and p2.state == 'aborted':
        return False
  return True

eventually always assertion Terminated:
  return coordinator.state in ('committed', 'aborted')


action Init:
  participants = []
  for i in range(NUM_PARTICIPANTS):
    p = Participant(ID=i)
    participants.append(p)

  coordinator = Coordinator()
```

Another example:

```
---
deadlock_detection: false
options:
    max_actions: 100
    crash_on_yield: false
    max_concurrent_actions: 3
action_options:
    SlateDb.Put:
        max_actions: 3
    SlateDb.FlushWal:
        max_concurrent_actions: 1
    SlateDb.FlushMemtable:
        max_concurrent_actions: 1

---

# Acts as tombstone for deletes and return of Get when key is not found
NOT_FOUND = "notfound"

KEYS = ["k0", "k1"]
VALUES = ["v0", "v1", NOT_FOUND]
# KEYS = ["k0", "k1"]
# VALUES = ["v0", "v1"]

ReadLevel = enum('Committed', 'Uncommitted')


role SlateDb:
  action Init:
    self.wal = {}
    # immutable_wal is a list of tuples (wal_index, data)
    # For now, we are limiting to a single immutable_wal, in the implementation
    # there can be multiple immutable_wals.
    self.immutable_wal = []

    self.memtable = {}
    self.immutable_memtable = {}

    self.l0 = []

    # For now, modeling only a single writer that maintains its own count
    self.wal_index = 1

  atomic action Put:
    require (self.MODE == 'RW')
    k = any KEYS
    v = any VALUES
    self.put(k, v)
    _last_puts[k] = v


  atomic action Get:
    k = any KEYS
    read_level = any dir(ReadLevel)
    v = writer.get(k, read_level)
    # k_level = any [ (k,l) for k in KEYS for l in dir(ReadLevel) ]
    # v = writer.get(k_level[0], k_level[1])
    return v

  fair action FlushWal:
    require len(self.wal) > 0 or len(self.immutable_wal) > 0
    self.freeze_wal()
    self.write_wal()
    self.update_memtable()
    self.immutable_wal = []


  fair action FlushMemtable:
    require len(self.memtable) > 0 or len(self.immutable_memtable) > 0
    self.freeze_memtable()
    self.write_l0()

    self.immutable_memtable.clear()


  atomic func put(k, v):
    self.wal[k] = v

  atomic func get(k, read_level):
    if read_level == ReadLevel.Uncommitted:
        if self.wal.get(k):
            return self.wal.get(k)
        if len(self.immutable_wal) > 0 and self.immutable_wal[0][1].get(k):
            return self.immutable_wal[0][1].get(k)


    if self.memtable.get(k):
        return self.memtable.get(k)
    if self.immutable_memtable.get(k):
        return self.immutable_memtable.get(k)

    for l0 in self.l0:
        content = store.Read(l0)
        if content and content.get(k):
            return content.get(k)

    return NOT_FOUND

  atomic func freeze_wal():
    if len(self.immutable_wal) > 0 or len(self.wal) == 0:
        return
    wal_copy = dict(self.wal)
    self.immutable_wal.append((self.wal_index, wal_copy))
    self.wal.clear()
    self.wal_index += 1

  atomic func write_wal():
    if len(self.immutable_wal) > 0 and len(self.immutable_wal[0][1]) > 0:
        store.Write("wal/" + format_with_leading_zeros(self.immutable_wal[0][0]) + ".sst", dict(self.immutable_wal[0][1]))


  atomic func update_memtable():
    if len(self.immutable_wal) == 0:
        return
    for kv in self.immutable_wal[0][1].items():
        self.memtable[kv[0]] = kv[1]


  atomic func freeze_memtable():
    if len(self.immutable_memtable) > 0 or len(self.memtable) <= 0:
        return

    self.immutable_memtable = dict(self.memtable)
    self.memtable.clear()

  atomic func write_l0():
    if len(self.immutable_memtable) == 0:
        return
    name = "compacted/ulid-" + format_with_leading_zeros(next_ulid) + ".sst"
    next_ulid += 1
    store.Write(name, dict(self.immutable_memtable))
    self.l0.insert(0, name)


  atomic func clear_immutable_memtable():
    self.immutable_memtable.clear()


role ObjectStore:
  action Init:
    self.objects = {}

  atomic func Write(name, content):
    self.objects[name] = content

  atomic func Read(name):
    return self.objects.get(name)


action Init:
  writer = SlateDb(MODE="RW")
  store = ObjectStore()
  # Slatedb uses ULID, that is a sortable unique id. We can model it with just a counter
  next_ulid = 1

  # As a convention, variables starting with _ are not part of the system modelled, but
  # useful for assertions
  _last_puts = {}


always assertion NoEmptyL0s:
    for l0 in writer.l0:
        if store.objects.get(l0) != None and len(store.objects.get(l0)) == 0:
            return False
    return True

always assertion UncommittedRead:
    for k in KEYS:
        v0 = writer.get(k, ReadLevel.Uncommitted)
        v1 = _last_puts.get(k, NOT_FOUND)
        if v0 != v1:
            return False
    return True

always eventually assertion CommittedRead:
    for k in KEYS:
        v0 = writer.get(k, ReadLevel.Uncommitted)
        v1 = writer.get(k, ReadLevel.Committed)
        v2 = _last_puts.get(k, NOT_FOUND)
        if v0 != v1 or v0 != v2:
            return False
    return True

always eventually assertion ConsistentRead:
    reader = SlateDb(mode="RO")
    reader.l0 = writer.l0
    for k in KEYS:
        v0 = writer.get(k, ReadLevel.Committed)
        v1 = reader.get(k, ReadLevel.Committed)
        if v0 != v1:
            return False
    return True

always assertion MaxObjects:
    return len(store.objects) <= 10

always eventually assertion WalFlushed:
    return len(writer.wal) == 0 and len(writer.immutable_wal) == 0

always eventually assertion MemtableFlushed:
    return len(writer.memtable) == 0 and len(writer.immutable_memtable) == 0

def format_with_leading_zeros(n, width=4):
    """
    Helper function to format integer with leading zeros.
    Starlark doesn't support python's format strings yet.
    """
    s = str(n)
    return "0" * (width - len(s)) + s
```

---
Some examples how to approach given a design description leading to full example, and some extensions.

Quick start guide using gossip protocol.

---
title: "Quick Start: Modeling and Validating Distributed Systems in FizzBee"
description: "Model and validate a distributed system using FizzBee by building a simple gossip protocol. This tutorial walks through defining system behavior, identifying design flaws, and visualizing interactions, providing an approachable introduction to formal methods and hands-on experience with formal modeling."
weight: -25
---

{{< toc >}}

## Introduction

In this tutorial, we’ll model a simplified distributed system using FizzBee, providing an accessible introduction to formal modeling. The goal is to show how formal methods can help engineers model and validate system behavior in a structured and practical way, without requiring deep prior knowledge of formal methods.

## The Gossip Protocol

A gossip protocol is a communication mechanism where nodes (servers) periodically exchange information with randomly selected peers. Over time, this ensures that all nodes converge to a consistent state.

### Simplified Gossip Protocol

- **N servers**, each with a data version (an integer, starting at 1).
- Each server keeps an **in-memory cache** of known versions from other servers.
- Periodically, a server picks another server and **exchanges information**.
- The goal is for all servers to eventually converge on the **latest version** of all other servers.

## Overview of the Process

In this tutorial, we’ll walk through the process of modeling and validating a distributed system using FizzBee.

{{< hint type=note >}}
The steps are broken down into smaller, digestible chunks to ensure clarity for beginners with little to no prior experience in formal methods. Formal methods experts may choose to combine or skip certain steps as needed, but the goal here is to make the process accessible and practical for all levels.
{{< /hint >}}

The process includes:

1. **Draw the Rough Design**: Explicitly define what the system should look like. This step helps prevent writer’s block when starting the first model by eliminating the confusion between *what* to model and *how* to model it. By outlining a rough design, you create a clear foundation, making the transition to formal modeling smoother.
2. **Translate to FizzBee Spec**: Convert the rough design into a corresponding FizzBee specification, mapping nodes to roles and selecting appropriate data structures.
3. **Run the Model Checker**: Use FizzBee’s model checker to validate the design and generate initial diagrams.
4. **Define Methods & Call Interactions**: Identify the key methods and model how services communicate with each other.
5. **Translate Pseudo Code to FizzBee Statements**: Express the system’s pseudocode as formal FizzBee statements.
6. **Review Sequence Diagrams & State Changes**: Analyze the generated sequence diagrams and check for expected state transitions.
7. **Add Durability & Ephemeral Annotations**: Mark the system’s components to differentiate between persistent and temporary states.
8. **Define Safety & Liveness Properties**: Specify the system’s safety and liveness requirements to ensure correctness.
9. **Share Your Spec for Review**: Once your spec is ready, share it with your team for feedback.

## Rough Design of the Gossip Protocol

For our protocol, the states will look something like this.
- There are 3 servers, each with a version number.
- There is a cache. For now, representing it as a list indexed by the server index, it could be a key value pair
- They all can talk to each other.

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/gossip-durability.png" alt="Gossip Protocol Rough Design" caption="Rough Design of the Gossip Protocol" >}}

## Translate to FizzBee Spec

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/draw-an-owl-meme.jpg" alt="How to draw an Owl Meme" >}}

There is only one type of service in our design, so we can define a single role `Server`.

```python
role Server:
    pass  # Empty block, we'll replace this later
```

{{< hint type="note" >}}
`pass` in fizzbee is the same as `pass` in python. It is a placeholder for an empty statement or an block.
Equivalent to `{}` or `;` in C, Java, and other languages.

{{< /hint >}}

Now, we just need to create three servers and initialize them.

```python
NUM_SERVERS=3

action Init:
    servers = []
    for i in range(NUM_SERVERS):
        servers.append(Server())
```

The Init action is the starting point of the model. It initializes the system by creating three servers and storing them in a list.

Any state variable at the end of the scope of Init is considered part of the state of the system.

{{% fizzbee %}}
---
# Temporarily disable deadlock detection.
deadlock_detection: false
---

NUM_SERVERS=3

role Server:
    pass

action Init:
    servers = []
    for i in range(NUM_SERVERS):
        servers.append(Server())

{{% /fizzbee %}}

FizzBee defines the state transitions to model and validate the design. Once the system is initialized,
we haven't defined what next steps can happen. So, according to the protocol it is considered a deadlock.

Disable this deadlock detection for now, by specifying the `deadlock_detection: false` in the YAML config.
The config can be added as a frontmatter.

### Run the model checker and see the results.
Check the `Enable Whiteboard` checkbox and click `Run` in the playground. Then click the `explorer` link.
This will show a block diagram like this.

{{% graphviz %}}
# skipped generated graphviz output

{{% /graphviz %}}

Indicating, there are three servers. Each of these servers do not have any states.

### Define the states

There are two variables, `version` and `cache`. The `version` is an integer, and the `cache` is a list of integers.

The expressions are starlark (a limited Python subset) statements. Similar to Python's __init__ function of a class,
the state variables will be defined in the `Server` role's `Init` action.

```python
role Server:
    action Init:
        self.version = 1
        self.cache = [0] * NUM_SERVERS
```
The spec will now look like this.

{{< hint type="note" >}}
The `self` keyword is used to refer to the current instance of the class.
It is similar to `self` in Python or `this` in other languages like Java, C++, etc.

Also note, we don't need the `pass` statement anymore as role block is not empty anymore.

{{< /hint >}}

{{% fizzbee %}}
---
deadlock_detection: false
---

NUM_SERVERS=3

role Server:
    action Init:
        self.version = 1
        self.cache = [0] * NUM_SERVERS

action Init:
    servers = []
    for i in range(NUM_SERVERS):
      servers.append(Server())
{{% /fizzbee %}}

Now, run the model and check the explorer. You should see,

{{% graphviz %}}
# skipped generated graphviz output

{{% /graphviz %}}

## Define the RPC methods and actions
In the gossip protocol, we expect only one RPC method. Let's call it `gossip()` in each of these servers.

```python
    func gossip(cache):
        pass  # Empty block, we'll fill this later
```
Someone should trigger this. Let's add a `GossipTimer` action to the `Server` role.

An action is almost similar to the `func`. The only difference is the func are called by other functions or actions,
where as actions are called by the some part of the system that is outside what we are modelling.
For example, `action` could represent an external client event, a timer, or user action and so on.

The GossipTimer will trigger the `gossip` method in an arbitrarily selected server.

```python

    action GossipTimer:
        i = any range(NUM_SERVERS)
        server = servers[i]
        server.gossip(cache)
```

Here, `any` keyword indicates, we are selection one of the elements on the collection arbitrarily.
The model checker will check for all possible values of `i` and `server`.

{{% fizzbee %}}

NUM_SERVERS=3

role Server:
    action Init:
        self.version = 1
        self.cache = [0] * NUM_SERVERS

    action GossipTimer:
        i = any range(NUM_SERVERS)
        server = servers[i]
        server.gossip(self.cache)

    func gossip(cache):
        pass  # Empty block, we'll fill this later


action Init:
    servers = []
    for i in range(NUM_SERVERS):
        servers.append(Server())

{{% /fizzbee %}}

{{< hint type="note" >}}
We can now remove the line that disables deadlock detection, because there is a possible action that can be triggered.
So, no chance of a deadlock.
{{< /hint >}}

Now, run the model checker. And open the explorer. This time you should see 3 buttons corresponding to the `GossipTimer` action.
Click one of those `Server#0.GossipTimer`. It will then ask to choose the `i` value, indicating which
server the node should gossip with.
Select `Any:i=1` or `Any:i=2`, you'll see the sequence diagram

Once the `server` is selected, continue the thread by clicking `action-1`, this will show the `gossip` method being called
in the sequence diagram.

{{< mermaid class="text-center" >}}
sequenceDiagram
	note left of 'Server#0': GossipTimer
	'Server#0' -&gt;&gt; 'Server#1': gossip(cache: [0, 0, 0])
	'Server#1' --&gt;&gt; 'Server#0': .
{{< /mermaid >}}

Play around by clicking various buttons to explore
(but there is nothing much interesting yet, as we don't have any logic).

However, one thing you'll notice is, when choosing the server to gossip with, each server can gossip with itself.
This is not an expected behavior. So, we need a way to exclude the current server from the selection.

So, we need an ID for each server.
When, creating a server, we can pass the ID as a parameter when creating the new server.

```udiff
21c21
<         servers.append(Server())
---
>         servers.append(Server(ID=i))
```

The ID parameter can be accessed with `self.ID` automatically.

At the `any` statement, include the `if` condition.

```diff
@@ -9,3 +9,3 @@
     action GossipTimer:
-        i = any range(NUM_SERVERS)
+        i = any range(NUM_SERVERS) if i != self.ID
         server = servers[i]
```

This will exclude the current server from the selection.

{{% fizzbee %}}

NUM_SERVERS=3

role Server:
    action Init:
        self.version = 1
        self.cache = [0] * NUM_SERVERS

    action GossipTimer:
        i = any range(NUM_SERVERS) if i != self.ID
        server = servers[i]
        server.gossip(self.cache)

    func gossip(cache):
        pass  # Empty block, we'll fill this later


action Init:
    servers = []
    for i in range(NUM_SERVERS):
        servers.append(Server(ID=i))
{{% /fizzbee %}}

While, during the exploration, you might notice that you can have multiple concurrent `GossipTimer` running
that is acceptable. But can a single server run multiple `GossipTimer` concurrently?
In a typical implementation, this is usually not done. So we can simply restrict the number of concurrent actions for each server's `GossipTimer` to 1.

```yaml
---
action_options:
    "Server#.GossipTimer":
        max_concurrent_actions: 1
---
```

Now, we have go the states modeled and started on the interactions. We now just need to fix
what actually happens when gossipping.

## Translate Pseudo Code to FizzBee Statements
It is time to fill in the logic of the `gossip` protocol in both the sending and receiving pairs.

**Sender:**
- Before sending its info, the sender should set its version to the cache.
- On receiving, the receiving the response from the receiver, the sender should update its cache with the received version.

**Receiver:**
- On receiving the version from the sender, the receiver should update its cache with the received version.
- And before sending the response, the receiver should set its version to the cache.

In both the cases, the merge operation is selecting the maximum of its current cached version and the received version.

As the syntax is similar to Python (actually starlark), we can use the `max` function to get the maximum of two values.

```python
    self.cache = [max(self.cache[i], received_cache[i]) for i in range(NUM_SERVERS)]
```

{{< hint type="warning" >}}
Even though in python this will be equivalent to,
```python
    for i in range(NUM_SERVERS):
        self.cache[i] = max(self.cache[i], received_cache[i])
```
There model checking semantics is different. If you change it to this, it will take a lot of time to model check, because
the model checker will explore scenarios where there is a context switching after each iteration.
To be equivalent, we need to say the entire list is updated at once by marking this atomic.
```python
    atomic for i in range(NUM_SERVERS):
        self.cache[i] = max(self.cache[i], received_cache[i])
```
{{< /hint >}}

Now, we can fill in the `GossipTimer` on the sender and `gossip` function for the receiver.

{{% fizzbee %}}
---
action_options:
       "Server#.GossipTimer":
              max_concurrent_actions: 1
---

NUM_SERVERS=2  # Reducing the number of servers to 2 temporarily. We will increase it later.

role Server:
    action Init:
        self.version = 1
        self.cache = [0] * NUM_SERVERS

    action GossipTimer:
        i = any range(NUM_SERVERS) if i != self.ID
        server = servers[i]
        self.cache[self.ID] = self.version
        received_cache = server.gossip(self.cache)
        self.cache = [max(self.cache[i], received_cache[i]) for i in range(NUM_SERVERS)]

    func gossip(received_cache):
        self.cache = [max(self.cache[i], received_cache[i]) for i in range(NUM_SERVERS)]
        self.cache[self.ID] = self.version
        return self.cache


action Init:
    servers = []
    for i in range(NUM_SERVERS):
        servers.append(Server(ID=i))


{{% /fizzbee %}}

At this point, the model checker will start taking a bit longer to run.

{{< hint type="warning" >}}
The online playground has limited capacity, and it was designed mainly for learning and small models.
If you can run it on your local machine, you can increase the number of servers to 3 or 4.

Minimum of 3 servers are required to find the usable benefit of the gossip protocol. And a minimum of 4 servers
are required to test for the correctness of the protocol.
{{< /hint >}}

Run the model checker with NUM_SERVERS=2. You should see the sequence diagram like this.

{{< mermaid class="text-center" >}}
sequenceDiagram
	note left of 'Server#0': GossipTimer
	'Server#0' -&gt;&gt; 'Server#1': gossip(received_cache: [1, 0])
	'Server#1' --&gt;&gt; 'Server#0': ([1, 1])
{{< /mermaid >}}

And a states visualization like this.

{{% graphviz %}}
# skipped generated graphviz output

{{% /graphviz %}}

## Reducing the state space by reducing the concurrency levels
Sometimes, when the model checker takes too long, we might still want to check for some scenarios where
things might go wrong even when there is no concurrency.
This may not test for all possible scenarios, but it is a good start. For some applications, you might need
to test with larger concurrency settings to find the issues.

For now, let us limit the number of actions that can be run concurrently to 1.

```yaml
---
options:
       max_concurrent_actions: 1
action_options:
       "Server#.GossipTimer":
              # This is not needed, as global max_concurrent_actions will be applied to all actions.
              max_concurrent_actions: 1
---
```
With this setting, if you run with 2 servers, it will produce around 70 states and
with 3 servers, you will get around 2500 states.

{{% mermaid class="text-center" %}}
sequenceDiagram
	note left of 'Server#0': GossipTimer
	'Server#0' -&gt;&gt; 'Server#1': gossip(received_cache: [1, 0, 0])
	'Server#1' --&gt;&gt; 'Server#0': ([1, 1, 0])
	note left of 'Server#2': GossipTimer
	'Server#2' -&gt;&gt; 'Server#1': gossip(received_cache: [0, 0, 1])
	'Server#1' --&gt;&gt; 'Server#2': ([1, 1, 1])
	note left of 'Server#1': GossipTimer
	'Server#1' -&gt;&gt; 'Server#0': gossip(received_cache: [1, 1, 1])
	'Server#0' --&gt;&gt; 'Server#1': ([1, 1, 1])
{{% /mermaid %}}

## Second action: BumpVersion
Now, let us add another action, `BumpVersion` to the `Server` that will increment the version number of the server.

```python
    action BumpVersion:
        self.version += 1
```
There is an issue here. The `version` can keep incrementing indefinitely. FizzBee is an explicit model checker,
it tries to explore all possible states. So, if the version keeps incrementing, it will keep exploring new states.
This is not practical. So, we need to limit the version number to a certain value.

Also, the state space increases exponentially with the number of servers and the version number. So, we should keep
the max version low enough that it still covers the scenarios we want to test.

For now, let us limit the number of times the version can be bumped. This can easily be set in the yaml frontmatter.

```yaml
options:
    max_concurrent_actions: 1
action_options:
    "Server.BumpVersion":
        max_actions: 1
    "Server#.GossipTimer":
        max_concurrent_actions: 1
```
Notice the `Server.BumpVersion`. This means, the `BumpVersion` action can only be run once acros all `Server` instances.
Whereas, if it was `Server#.BumpVersion`, it would mean, each `Server` instance can run the `BumpVersion` action once.

Now, we run the model checker, and explore the states.

{{% fizzbee %}}
---
options:
    max_concurrent_actions: 1
action_options:
    "Server.BumpVersion":
        max_actions: 1
    "Server#.GossipTimer":
        max_concurrent_actions: 1
---

NUM_SERVERS=3

role Server:
    action Init:
        self.version = 1
        self.cache = [0] * NUM_SERVERS

    action BumpVersion:
        self.version += 1

    action GossipTimer:
        i = any range(NUM_SERVERS) if i != self.ID
        server = servers[i]
        self.cache[self.ID] = self.version
        received_cache = server.gossip(self.cache)
        self.cache = [max(self.cache[i], received_cache[i]) for i in range(NUM_SERVERS)]

    func gossip(received_cache):
        self.cache = [max(self.cache[i], received_cache[i]) for i in range(NUM_SERVERS)]
        self.cache[self.ID] = self.version
        return self.cache


action Init:
    servers = []
    for i in range(NUM_SERVERS):
        servers.append(Server(ID=i))

{{% /fizzbee %}}

Play with the sequence diagram to see how the states change. One example here.
```
s0 <---> s1
s0 increments version
s1 <---> s2
s0 <---> s2
At this point,
the stats will be [2, 1, 1], [1, 1, 1], [2, 1, 1].
```
{{% mermaid class="text-center" %}}
sequenceDiagram
	note left of 'Server#0': GossipTimer
	'Server#0' -&gt;&gt; 'Server#1': gossip(received_cache: [1, 0, 0])
	'Server#1' --&gt;&gt; 'Server#0': ([1, 1, 0])
	note left of 'Server#0': BumpVersion
	note left of 'Server#1': GossipTimer
	'Server#1' -&gt;&gt; 'Server#2': gossip(received_cache: [1, 1, 0])
	'Server#2' --&gt;&gt; 'Server#1': ([1, 1, 1])
	note left of 'Server#0': GossipTimer
	'Server#0' -&gt;&gt; 'Server#2': gossip(received_cache: [2, 1, 0])
	'Server#2' --&gt;&gt; 'Server#0': ([2, 1, 1])
{{% /mermaid %}}
And the state spaces.

{{% graphviz %}}
# skipped generated graphviz output

{{% /graphviz %}}


## Add Durability and Ephemeral Annotations
In our design, the `version` is a durable state, and the `cache` is ephemeral. In FizzBee, by default
all state are assumed to be durable. To mark the `cache` as ephemeral, we just need to set an annotation.

```python
@state(ephemeral=["cache"])
role Server:
    # other code
```



{{% fizzbee %}}
---
options:
    max_concurrent_actions: 1
action_options:
    "Server.BumpVersion":
        max_actions: 1
    "Server#.GossipTimer":
        max_concurrent_actions: 1
---

NUM_SERVERS=3

@state(ephemeral=["cache"])
role Server:
    action Init:
        self.version = 1
        self.cache = [0] * NUM_SERVERS

    action BumpVersion:
        self.version += 1

    action GossipTimer:
        i = any range(NUM_SERVERS) if i != self.ID
        server = servers[i]
        self.cache[self.ID] = self.version
        received_cache = server.gossip(self.cache)
        self.cache = [max(self.cache[i], received_cache[i]) for i in range(NUM_SERVERS)]

    func gossip(received_cache):
        self.cache = [max(self.cache[i], received_cache[i]) for i in range(NUM_SERVERS)]
        self.cache[self.ID] = self.version
        return self.cache


action Init:
    servers = []
    for i in range(NUM_SERVERS):
        servers.append(Server(ID=i))


{{% /fizzbee %}}
Now, when you run the model checker and try the state explorer, you will see a few more action buttons titled
`crash-role Server#0`, `crash-role Server#0` and `crash-role Server#0`. When you click one of these,
it simulates a process restart. You will see the ephemeral data (`cache`) wiped out to its initial state
but the `version` would be preserved.

For example, lets get to this state.

{{% mermaid class="text-center" %}}
sequenceDiagram
	note left of 'Server#0': BumpVersion
	note left of 'Server#0': GossipTimer
	'Server#0' -&gt;&gt; 'Server#1': gossip(received_cache: [2, 0, 0])
	'Server#1' --&gt;&gt; 'Server#0': ([2, 1, 0])
{{% /mermaid %}}
The state would look something like this.

{{% graphviz %}}
# skipped generated graphviz output

{{% /graphviz %}}

Now, click on `crash-role Server#0`, the new state would look like,
{{% graphviz %}}
# skipped generated graphviz output
{{% /graphviz %}}

Specifically, I want you to notice the values of the cache in `Server#0` before and after.
Notice, the `version=2` as version is durable and `cache=[0, 0, 0]` as cache is ephemeral.

## Add Safety and Liveness Properties

### Safety property
Safety invariants are those that must always be true.
For the gossip protocol, the safety property is not that relevant, but just as an example, we will add it here.

The cached version of any server should never be less than the remote server's version.

We can represent it with
```python
always assertion CachedVersionBelowActualVersion:
    return all([server.cache[i] <= servers[i].version for server in servers for i in range(NUM_SERVERS)])
```
For forks, not familiar with Python, this is the same as
```python
always assertion CachedVersionBelowActualVersion:
    for server in servers:
        for i in range(NUM_SERVERS):
            if server.cache[i] > servers[i].version:
                return False
    return True
```
Due to the implementation details, the first version runs much faster than the second one.

### Liveness property
Liveness properties are those that must eventually be true. While safety property actually says, nothing bad ever happens,
liveness property says, something good eventually happens.

For the gossip protocol, we can say, eventually, all servers should have the same version.

```python

always eventually assertion ConsistentCache:
  return all([server.cache[i] == servers[i].version for server in servers for i in range(NUM_SERVERS)])

```
{{% fizzbee %}}
---
options:
    max_concurrent_actions: 1
action_options:
    "Server.BumpVersion":
        max_actions: 1
    "Server#.GossipTimer":
        max_concurrent_actions: 1
---

NUM_SERVERS=3

role Server:
    action Init:
        self.version = 1
        self.cache = [0] * NUM_SERVERS
        # self.cache[self.ID] = self.version

    action BumpVersion:
        self.version += 1

    action GossipTimer:
        i = any range(NUM_SERVERS) if i != self.ID
        server = servers[i]
        self.cache[self.ID] = self.version
        received_cache = server.gossip(self.cache)
        self.cache = [max(self.cache[i], received_cache[i]) for i in range(NUM_SERVERS)]

    func gossip(received_cache):
        self.cache = [max(self.cache[i], received_cache[i]) for i in range(NUM_SERVERS)]
        self.cache[self.ID] = self.version
        return self.cache



always eventually assertion ConsistentCache:
  return all([server.cache[i] == servers[i].version for server in servers for i in range(NUM_SERVERS)])

action Init:
    servers = []
    for i in range(NUM_SERVERS):
        servers.append(Server(ID=i))


{{% /fizzbee %}}

When you run it, you will see an error. The trace wil say,

```
FAILED: Liveness check failed
Invariant: ConsistentCache
------
Init
--
state: {"servers":"[role Server#0 (params(ID = 0),fields(cache = [0, 0, 0], version = 1)), role Server#1 (params(ID = 1),fields(cache = [0, 0, 0], version = 1)), role Server#2 (params(ID = 2),fields(cache = [0, 0, 0], version = 1))]"}
------
stutter
--
state: {"servers":"[role Server#0 (params(ID = 0),fields(cache = [0, 0, 0], version = 1)), role Server#1 (params(ID = 1),fields(cache = [0, 0, 0], version = 1)), role Server#2 (params(ID = 2),fields(cache = [0, 0, 0], version = 1))]"}
------

```

### Stuttering and Fairness
Our model defines what *can* happen, but liveness properties require that certain actions *do* happen.

In our gossip protocol, skipping `BumpVersion` is fine, but `GossipTimer` must eventually occur for liveness to hold.
Developers must ensure this in implementation. [Learn more about fairness](https://fizzbee.io/design/tutorials/getting-started/#fairness).
For this spec, we just need this to be weakly fair that is the default and most common fairness.

```diff
diff tmp/spec1.txt tmp/spec2.txt
22c22
<     action GossipTimer:
---
>     fair action GossipTimer:

```

Now, when you run, you won't see any error.

Now, try increasing the number of servers to 4. You will see the liveness property failing.
But unfortunately, in the online playground the number of states would have exceeded so high, it will timeout.

We can temporarily reduce the statespace further by marking the gossip action as atomic and probably remove the BumpVersion action
as well.

```diff
11c11
< NUM_SERVERS=3
---
> NUM_SERVERS=4
19,22c19,21
<     action BumpVersion:
<         self.version += 1
<
<     fair action GossipTimer:
---
>     # Marking this `atomic` is wrong as it implies it can load and update the cache and invoke the remove rpc in a single atomic step.
>     # We do it anyway to temporarily
>     atomic fair action GossipTimer:
29c28,29
<     func gossip(received_cache):
---
>     # Marking this `atomic` is wrong
>     atomic func gossip(received_cache):
```

So, run this spec.

{{% fizzbee %}}
---
options:
    max_concurrent_actions: 1
action_options:
    "Server.BumpVersion":
        max_actions: 1
    "Server#.GossipTimer":
        max_concurrent_actions: 1
---

NUM_SERVERS=4

role Server:
    action Init:
        self.version = 1
        self.cache = [0] * NUM_SERVERS
        # self.cache[self.ID] = self.version

    # Marking this `atomic` is wrong as it implies it can load and update the cache and invoke the remove rpc in a single atomic step.
    # We do it anyway to temporarily
    atomic fair action GossipTimer:
        i = any range(NUM_SERVERS) if i != self.ID
        server = servers[i]
        self.cache[self.ID] = self.version
        received_cache = server.gossip(self.cache)
        self.cache = [max(self.cache[i], received_cache[i]) for i in range(NUM_SERVERS)]

    # Marking this `atomic` is wrong
    atomic func gossip(received_cache):
        self.cache = [max(self.cache[i], received_cache[i]) for i in range(NUM_SERVERS)]
        self.cache[self.ID] = self.version
        return self.cache



always eventually assertion ConsistentCache:
  return all([server.cache[i] == servers[i].version for server in servers for i in range(NUM_SERVERS)])

action Init:
    servers = []
    for i in range(NUM_SERVERS):
        servers.append(Server(ID=i))


{{% /fizzbee %}}

When you run this spec, you'll see an error. The generated trace for liveness is not the shortest at this moment.
But if you open the error graph, and scroll to the bottom right, you will see, there is a possibility that
two servers might choose each other and form a partition to gossip, and so the information about
some server do not reach some others.
That is because, even though all servers are gossiping as we marked them fair, we didn't specify
how they should select the server to communicate with. Specifically this line.

```python
        i = any range(NUM_SERVERS) if i != self.ID
```
That line says select any server other than itself. There is no mention of how to select the server to gossip with.

A simplied reproduction of the scenario is,
{{% mermaid class="text-center" %}}
sequenceDiagram
	note left of 'Server#0': GossipTimer
	'Server#0' -&gt;&gt; 'Server#1': gossip(received_cache: [1, 0, 0, 0])
	'Server#1' --&gt;&gt; 'Server#0': ([1, 1, 0, 0])
	note left of 'Server#2': GossipTimer
	'Server#2' -&gt;&gt; 'Server#3': gossip(received_cache: [0, 0, 1, 0])
	'Server#3' --&gt;&gt; 'Server#2': ([0, 0, 1, 1])
	note left of 'Server#1': GossipTimer
	'Server#1' -&gt;&gt; 'Server#0': gossip(received_cache: [1, 1, 0, 0])
	'Server#0' --&gt;&gt; 'Server#1': ([1, 1, 0, 0])
	note left of 'Server#3': GossipTimer
	'Server#3' -&gt;&gt; 'Server#2': gossip(received_cache: [0, 0, 1, 1])
	'Server#2' --&gt;&gt; 'Server#3': ([0, 0, 1, 1])
{{% /mermaid %}}

Imagine, in the implementation, each server maintains a `set` of urls for other servers, and it selects the first one each time.
So this could cause the liveness issue. To fix this, in the implementation, all we need to do is,
select a random server from the set of urls.
That is, we will make a fair selection of the server to gossip with.

```diff
22c22
<         i = any range(NUM_SERVERS) if i != self.ID
---
>         i = fair any range(NUM_SERVERS) if i != self.ID
```
Now, when you run, the liveness check will succeed.


### Make the actions atomic for quicker(probably incorrect) model checking
Sometimes, we might want to test for certain scenarios, in that cases it might be easier to keep the state spaces
low. For example,

```udiff
13c13
<     action GossipTimer:
---
>     atomic action GossipTimer:
20c20
<     func gossip(received_cache):
---
>     atomic func gossip(received_cache):
```
Now, with NUM_SERVERS=2, it will only produce 6 states, it says entire gossiping happens atomically.
Similarly, with NUM_SERVERS=3, it will produce 212 states.

Unfortunately, it is not practical to implement it that way.
This is an advantage of the visualizer as you could notice the entire rpc and state updates happen in
a single step. This is a modeling mistake I have noticed even among formal methods experts.

That said, occasionally it is nice to have a quick check because if things fail with atomic actions, it is definitely
fail with non-atomic.

## Share Your Spec for Review
Once your spec is ready, share it with your team for feedback. Unlike traditional prose design docs, a FizzBee spec is precise and interactive—your team can explore and understand the design more effectively.

For this tutorial, that also means sharing it on your social networks. Yes, really. 😄


=== Another Example with Two Phase commit protocol using the actor style that is the recommended approach.

---
title: Two Phase Commit (Actor model/Object oriented)
weight: 11
---

{{< toc >}}

# Two Phase Commit (Object oriented/Actor style implementation)

This is another implementation style in FizzBee to model the Two Phase Commit protocol.
There are two other styles,
1. [Functional style](https://fizzbee.io/design/examples/two_phase_commit/) similar to TLA+
2. [Procedural style](https://fizzbee.io/design/examples/two_phase_commit_procedural/) similar to PlusCal
3. Object oriented style (this post) similar to P

The significant benefit of this style is, it would feel more natural to how you think about the design of
the distributed systems.

For more information about Roles, refer to [Roles](https://fizzbee.io/design/tutorials/roles/).


## Quick overview

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit.png" alt="Two Phase Commit Protocol Rough Design" caption="Rough Design of the Two Phase Commit Protocol" >}}


## Summary of the steps to implement


### Preparation

0. **Start with some rough design diagrams:**
   Sketch a rough system design and sequence diagram to clarify roles and interactions.
   They don’t need to be precise or complete - this reduces cognitive load and helps you focus on the modeling process.

### Modelling

1. **Identify the roles:**

   As shown in the sequence diagram, there are two roles - `Coordinator` and `Participant`.
2. **Identify the state for each role:**

   Each `Coordinator` and `Participant` has its own `status` variable to track the state of the transaction independently.

3. **Define and initialize the system:**

   Create instances of `Coordinator` and `Participant` roles to represent the transaction system.

4. **Identify the RPCs for each role:**

   The `Participant` exposes two RPCs - `Prepare` (Phase 1) and `Finalize` (Phase 2).

    - **RPCs** are functions called between different roles.
    - In this case, `Prepare` and `Finalize` are RPCs because the `Coordinator` calls them on the `Participant`.
5. **Identify the actions for each role:**

    - **Actions** are similar to functions but do not specify a caller. They represent external triggers such as user requests, timers, or client API calls.
    - The `Coordinator` has an action `Commit`, which starts the two-phase commit protocol.
6. **Implement the RPCs and actions.**

   This is the core of the design specification-the pseudo-code part of the spec. The implementation is written in a Python-compatible Starlark-like language, defining the logic for each role.
   You can introduce helper functions, enforce preconditions, and model non-determinism where needed.

7. **Identify durable/ephemeral states:**

    In this implementation, the `status` variables of both `Coordinator` and `Participant` are durable. Since durability is the default, no additional handling is needed.

### Validation

8  **Identify the safety properties:**

    The critical safety property is: if any `Participant` commits the transaction, no other `Participant` must have aborted (and vice versa).
9. **Identify the liveness properties:**

    In the basic implementation, there are no liveness guarantees. However, we will later extend the protocol to ensure that no transaction remains indefinitely unresolved; every started transaction must eventually be `committed` or `aborted`.

## Modelling

### Roles

If you looked at the sequence diagram, you can see there are two roles or microservices or process types - Coordinator and Participant.

Roles are defined using the `role` keyword and the syntax is similar to a `class` declaration in python.
```python
role Coordinator:
    # ...

role Participant:
    # ...
```

### States

The state variables for each role is defined within `Init` action. To reference the state variable `status`, we will
use python like syntax `self.status`.

```python
role Coordinator:
    action Init:
        self.status = "init"

role Participant:
    action Init:
        self.status = "init"
```

### Defining the System and Initializing Roles

Before modeling behavior, we need to **define the system** - specifying the roles involved and initializing them.

In this case, the system consists of **one `Coordinator`** and **multiple `Participants`**.

Initializing a role is similar to calling a **constructor in Python**:
- `Coordinator()` creates a new coordinator instance.
- `Participant()` creates a new participant instance.

The `Init` action sets up the system:

```python

NUM_PARTICIPANTS = 2

action Init:
    coordinator = Coordinator()
    participants = []
    for i in range(NUM_PARTICIPANTS):
        participants.append(Participant())

```

Run the following code in the playgroud.
"Run in playground" will open the snippet in the playground.
Then, click "Enable whiteboard" checkbox and click Run.
{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/EnableWhiteboardAndRun.png" alt="Enable whiteboard and Run" caption="The screenshot of the playground highlighting the Enable whiteboard and Run steps" >}}

{{% fizzbee %}}
---
# Disable  deadlock detection for now. We will address it later.
deadlock_detection: false
---

role Coordinator:
    action Init:
        self.status = "init"

role Participant:
    action Init:
        self.status = "init"


NUM_PARTICIPANTS = 2

action Init:
    coordinator = Coordinator()
    participants = []
    for i in range(NUM_PARTICIPANTS):
        participants.append(Participant())

{{% /fizzbee %}}

When you run it, you'll see an `Explorer` link. Click you'll see an explorer.
{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/ClickExplorerLink.png" alt="Click Explorer Link to open the Explorer view" caption="The screenshot of the playground highlighting the Explorer link to open the Explorer view" >}}

At present, it will only show the init state.
As we start filling in the details, you'll see more interactive exploration.

{{% graphviz %}}
digraph G {
compound=true;
target="_blank";
subgraph "cluster_Coordinator#0" {
style=dashed;
label="Coordinator#0";
"Coordinator#0_placeholder" [label="" shape=point width=0 height=0 style=invis];
"Coordinator#0.status" [label="status = init" shape=ellipse];
}
subgraph "cluster_Participant#0" {
style=dashed;
label="Participant#0";
"Participant#0_placeholder" [label="" shape=point width=0 height=0 style=invis];
"Participant#0.status" [label="status = init" shape=ellipse];
}
subgraph "cluster_Participant#1" {
style=dashed;
label="Participant#1";
"Participant#1_placeholder" [label="" shape=point width=0 height=0 style=invis];
"Participant#1.status" [label="status = init" shape=ellipse];
}
"coordinator" [label="coordinator = role Coordinator#0" shape=ellipse];
"coordinator" -> "Coordinator#0_placeholder" [lhead="cluster_Coordinator#0"];
subgraph "cluster_null.participants" {
style=dashed;
label="participants";
"participants" [shape=plaintext, label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6"><tr>
<td port="before" sides="b"></td></tr>
<tr><td port="0">role Participant#0</td></tr>
<tr><td port="1">role Participant#1</td></tr>
<tr><td port="after" sides="t"></td>
</tr></table>>];
}
"participants":0 -> "Participant#0_placeholder" [lhead="cluster_Participant#0"];
"participants":1 -> "Participant#1_placeholder" [lhead="cluster_Participant#1"];
}
{{% /graphviz %}}


### RPCs

RPCs are just functions, with the only difference being these will typically be called from another role. That is,
inter-role function calls are treated as RPCs, whereas intra-role function calls are treated as just helper functions.

Functions are defined using `func` keyword (As opposed to `def` in python and the `self` parameter is implicit)

```python

role Participant:
    action Init:
        self.status = "init"

    func Prepare():
        pass # To be filled in later

    func Finalize():
        pass # To be filled in later

```

### Actions

Actions are defined using `action` keyword.

```python
role Coordinator:
    action Init:
        self.status = "init"

    action Commit:
        pass # To be filled in later

```

### Implement RPCs and Actions

#### Participant

The `Participant` exposes two RPCs:

- **`Prepare`** (Phase 1): The participant decides whether it can commit the transaction.
- **`Finalize`** (Phase 2): The participant records the final decision.

##### Prepare Phase
On `Prepare`, a participant can either **prepare to commit** or **abort** the transaction. While real-world implementations have specific reasons for aborting, **our model only needs to capture whether the decision is `aborted` or `prepared`**, not why.

To express this, we use **non-determinism** with the `any` keyword:

```python
vote = any ["prepared", "aborted"]
```
This means the vote can be either `"prepared"` or `"aborted"`, and the model checker will explore both cases to ensure correctness.

Once the vote is determined, the participant stores the decision in `self.status` and returns the vote.

##### Finalize Phase
The `Finalize` function takes a single parameter - the final transaction `decision`. This decision is persisted in `self.status` to reflect whether the transaction was ultimately committed or aborted.

Participant Implementation

```python
role Participant:
    action Init:
        self.state = "init"

    func Prepare():
        vote = any ["prepared", "aborted"]
        self.status = vote
        return vote

    func Finalize(decision):
        self.status = decision
```

#### Coordinator Action: Commit

The `Coordinator` has a **`Commit`** action that runs the Two-Phase Commit protocol.

To ensure the protocol starts only once, we add a precondition to trigger the action only when the status is `"init"`.
When triggered, the status is set to `"working"`.

```diff
     action Commit:
-        pass # To be filled in later
+        require(self.status == "init")
+        self.status = "working"
```

Next, the coordinator iterates over the participants' list. For each participant, it calls **`Prepare`**.
If any participant votes `"aborted"`, the coordinator sets the status to `"aborted"`, triggers the second phase, and exits.
If all participants accept, it sets the status to `"committed"`.

```python
for p in participants:
    vote = p.Prepare()
    if vote == "aborted":
        self.finalize("aborted")
        return

self.finalize("committed")

```

The **`finalize`** function handles the second phase, persisting the decision and notifying each participant.

```python
func finalize(decision):
    self.status = decision
    for p in participants:
        p.Finalize(decision)
```

Coordinator implementation:

```python
role Coordinator:
    action Init:
        self.status = "init"

    action Commit:
        require(self.status == "init")
        self.status = "working"

        for p in participants:
            vote = p.Prepare()
            if vote == "aborted":
                self.finalize("aborted")
                return

        self.finalize("committed")

    func finalize(decision):
        self.status = decision
        for p in participants:
            p.Finalize(decision)

```

### Exploring the model
We now have a working description of what we want to design. Let us explore the design in the UI.

{{% fizzbee %}}

---
# Disable  deadlock detection for now. We will address it later.
deadlock_detection: false
---

role Coordinator:
    action Init:
        self.status = "init"

    action Commit:
        require(self.status == "init")
        self.status = "working"

        for p in participants:
            vote = p.Prepare()
            if vote == "aborted":
                self.finalize("aborted")
                return

        self.finalize("committed")

    func finalize(decision):
        self.status = decision
        for p in participants:
            p.Finalize(decision)

role Participant:
    action Init:
        self.state = "init"

    func Prepare():
        vote = any ["prepared", "aborted"]
        self.status = vote
        return vote

    func Finalize(decision):
        self.status = decision


NUM_PARTICIPANTS = 2

action Init:
    coordinator = Coordinator()
    participants = []
    for i in range(NUM_PARTICIPANTS):
        participants.append(Participant())


{{% /fizzbee %}}

You can see the state diagram and explore how the state space can change. Additionally, you may notice how faults, like a thread crash, are simulated.
[FizzBee automatically simulates various types of faults](https://fizzbee.io/design/tutorials/fault-injection/) to help model your system under these scenarios.

#### State graph
In the state diagram, you'll notice that when a thread crashes, the coordinator remains in the "working" state. However, there is no outgoing transition from this state, which implies a potential deadlock.

To allow for exploration of more interesting scenarios, we have temporarily disabled deadlock detection in this specification.

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-CrashDeadlock.png" alt="Crash modeling showing deadlock" caption="State diagram illustrating thread crash modeling and resulting deadlock" >}}

#### Communication diagram

{{% graphviz %}}
# generated graphviz output ignored

{{% /graphviz %}}

#### Explorer view

Now, open the explorer. On the left, you'll see the button labeled `"Coordinator#0.Commit"`, corresponding to the only action defined in the model.

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-ExplorerCommit.png" alt="Action button for Coordinator#0.Commit in Explorer view" caption="Explorer action button for Coordinator#0.Commit" >}}

Clicking it will change the Coordinator's state to `"working"`, which is the first statement in the `Commit` action.

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-CoordinatorWorking.png" alt="Coordinator#0.state = working in Explorer view" caption="Explorer view showing Coordinator#0.state = working" >}}

On the left, you'll see two buttons.

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-ExplorerAction1OrCrash.png" alt="Explorer showing action buttons with option for crash or action-1" caption="Explorer showing action buttons: 'action-1' or 'crash'" >}}

Clicking "action-1" will proceed with the thread, initiating the `Prepare` RPC to `Participant#0`.

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-ExplorerParticipantAnyStmt.png" alt="Explorer view showing Participant#0 status" caption="Explorer view showing Participant#0 status options" >}}

The first step for the `Participant` is choosing to either prepare or abort the transaction. Selecting `"Any:prepared"` shows the prepared path, changing `Participant#0.status` to `"prepared"` and returning.

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-ExplorerParticipant0Prepared.png" alt="Participant#0.status = prepared" caption="Explorer view showing Participant#0.status = prepared" >}}

You can explore the next steps and generate various sequence diagrams.

**All Committed**
{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-AllCommitted.png" alt="Explorer view showing all states committed" caption="Explorer view showing all states committed" >}}

**Aborted**
{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-TransactionAborted.png" alt="Explorer view showing transaction aborted" caption="Explorer view showing transaction aborted" >}}

**Crashed**
{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-TransactionCrashedAfter1Prepare.png" alt="Explorer view showing transaction crashed after 1st prepare" caption="Explorer view showing transaction crashed after 1st prepare" >}}

In this state, you'll see the participants remain stuck in `"prepared"` state.
{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-ParticipantsPreparedState.png" alt="Explorer view showing transaction crashed after 1st prepare" caption="Explorer view showing transaction crashed after 1st prepare" >}}



## Validation
While you can use FizzBee to model your system, explore and simulate various scenarios, and look at how the state variables change,
it would be nicer to add some assertions to the state.
This is called model checking.

### Safety invariant
The critical invariant is, to confirm there is no state where one participant committed but another participant aborted.

```python
always assertion ParticipantsConsistent:
    for p1 in participants:
        for p2 in participants:
            if p1.status == 'committed' and p2.status == 'aborted':
                return False
    return True
```
### Liveness invariant
While the safety property shows `ParticipantsConsistent`, there is no state where one participant committed but another one aborted.
However, this could be easily achieved with not having any actions (for example, comment out the `Commit` action) or just
trivially aborting all transactions. One thing we would want to verify is, is there at least one state were every participant committed.

```python
exists assertion AllParticipantsCommitted:
    for p1 in participants:
            if p1.status != 'committed':
                return False
    return True
```
Note, this is not an LTL property. This is part of the CTL logic equivalent to `EF p` where p is the predicate

## Extension: Timeout
In the current system, if the process crashes in between, the participants that `prepared` the transaction would be
stuck in that state forever. Ideally, we would want all the participants to either commit or abort.

Since commit cannot be guaranteed, one way to force convergence is to use a timeout.

### Liveness invariant

```python
eventually always assertion CoordinatorTerminated:
    return coordinator.status != 'working'
```

```python
eventually always assertion AllParticipantsTerminated:
    for p1 in participants:
        if p1.status == 'prepared':
            return False
    return True
```

When you run this, you will see the trace that when the node crashes, the system will not reach terminal state.

### Timeout attempt 1: Simply abort
Let us start with a naive attempt.

```python
role Coordinator:
    # ... other actions and functions
    action Timeout:
        self.finalize("aborted")
```

{{% fizzbee %}}


role Coordinator:
    action Init:
        self.status = "init"

    action Commit:
        require(self.status == "init")
        self.status = "working"

        for p in participants:
            vote = p.Prepare()
            if vote == "aborted":
                self.finalize("aborted")
                return

        self.finalize("committed")

    func finalize(decision):
        self.status = decision
        for p in participants:
            p.Finalize(decision)

    action Timeout:
        self.finalize("aborted")

role Participant:
    action Init:
        self.status = "init"

    func Prepare():
        vote = any ["prepared", "aborted"]
        self.status = vote
        return vote

    func Finalize(decision):
        self.status = decision


NUM_PARTICIPANTS = 2

action Init:
    coordinator = Coordinator()
    participants = []
    for i in range(NUM_PARTICIPANTS):
        participants.append(Participant())

always assertion ParticipantsConsistent:
    for p1 in participants:
        for p2 in participants:
            if p1.status == 'committed' and p2.status == 'aborted':
                return False
    return True

exists assertion AllParticipantsCommitted:
    for p1 in participants:
            if p1.status != 'committed':
                return False
    return True

eventually always assertion CoordinatorTerminated:
    return coordinator.status != 'working'

eventually always assertion AllParticipantsTerminated:
    for p1 in participants:
            if p1.status == 'prepared':
                return False
    return True


{{% /fizzbee %}}

You will see the following error trace.

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-timeout-after-commit.png" alt="Error trace showing timeout triggered after commit" caption="Error trace showing a timeout occurring after all participants have committed" >}}

The issue here is that even after all participants have committed, the timeout action can still be triggered, causing the transaction to start aborting.

You can see this behavior in the error details view:

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-timeoutAfterCommitErrorDetails.png" alt="Error details showing timeout triggered after commit" caption="Error details showing how a timeout is incorrectly triggered after commit" >}}

And it is also reflected in the sequence diagram:

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-timeoutAfterCommitSequenceDiagram.png" alt="Sequence diagram showing timeout triggered after commit" caption="Sequence diagram illustrating the timeout incorrectly triggering after commit" >}}

### Timeout attempt 2: Abort transaction if not committed

Add a precondition that only transaction that is not committed must be aborted.

```python
    action Timeout:
        require(self.status != "committed")    # <--- This is the only changed line here
        self.finalize("aborted")
```
While the previous error is fixed, this scenario reveals a different issue.

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-timeoutBeforePhase2.png" alt="Error trace showing timeout triggered before Phase 2" caption="Error trace showing a timeout occurring before Phase 2, leading to inconsistency" >}}

Once both participants have prepared but before the coordinator receives the last participant's response, if the timeout handler is triggered, it will decide to abort the transaction. Then, if the context switches back to the commit thread, it will proceed with Phase 2 and commit the transaction.

Once that is done, the timeout handler can still continue executing the abort path, leading to an inconsistent state. The error details view clearly highlights this context switching issue.

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-timeoutBeforePhase2Details.png" alt="Error details showing timeout triggered before Phase 2" caption="Error details highlighting how a timeout before Phase 2 leads to an inconsistent state" >}}

### Timeout attempt 3: Finalize only in working status

```python
    func finalize(decision):
        require(self.status == "working")   # <-- This is the only changed line
        self.status = decision
        for p in participants:
            p.Finalize(decision)
```

When you run it, it will show a new error.
{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-liveness-stutter-error.png" alt="Error details showing timeout triggered before Phase 2" caption="Error details highlighting how a timeout before Phase 2 leads to an inconsistent state" >}}

This time it is a liveness error caused by stuttering. The fix is simply
to mark the timeout action to be fair.

```udiff
@@ -25,7 +25,7 @@
         for p in participants:
             p.Finalize(decision)

-    action Timeout:
+    fair action Timeout:
         require(self.status != "committed")
         self.finalize("aborted")
```

This will now show a different error.
{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-timeout-thread-crashed-stutter-error.png" alt="Error details showing timeout triggered before Phase 2" caption="Error details highlighting how a timeout before Phase 2 leads to an inconsistent state" >}}

### Timeout attempt 4: Allow timeout to retry
Allow the finalize if the decision is the same as the previously decided value.

```diff

@@ -20,7 +20,7 @@
self.finalize("committed")

     func finalize(decision):
-        require(self.status == "working")
+        require(self.status in ["working", decision])
         self.status = decision
         for p in participants:
             p.Finalize(decision)
```
This will fix that, but show the scenario where the thread crashed after coordinator decided to commit.

{{< figure src="https://storage.googleapis.com/fizzbee-public/website/tutorials/img/TwoPhaseCommit-crashAfterCommit-NoTimeout.png" alt="Error details showing timeout triggered before Phase 2" caption="Error details highlighting how a timeout before Phase 2 leads to an inconsistent state" >}}

### Timeout attempt 5: Allow timeout handler to retry commit as well

```diff
     fair action Timeout:
-        require(self.status != "committed")
-        self.finalize("aborted")
+        if self.status == "committed":
+            self.finalize("committed")
+        else:
+            self.finalize("aborted")
```
This will fix the liveness issue as well.


## Complete code

{{% fizzbee %}}

role Coordinator:
    action Init:
        self.status = "init"

    action Commit:
        require(self.status == "init")
        self.status = "working"

        for p in participants:
            vote = p.Prepare()
            if vote == "aborted":
                self.finalize("aborted")
                return

        self.finalize("committed")

    func finalize(decision):
        require(self.status in ["working", decision])
        self.status = decision
        for p in participants:
            p.Finalize(decision)

    fair action Timeout:
        if self.status == "committed":
            self.finalize("committed")
        else:
            self.finalize("aborted")

role Participant:
    action Init:
        self.status = "init"

    func Prepare():
        vote = any ["prepared", "aborted"]
        self.status = vote
        return vote

    func Finalize(decision):
        self.status = decision


NUM_PARTICIPANTS = 2

action Init:
    coordinator = Coordinator()
    participants = []
    for i in range(NUM_PARTICIPANTS):
        participants.append(Participant())

always assertion ParticipantsConsistent:
    for p1 in participants:
        for p2 in participants:
            if p1.status == 'committed' and p2.status == 'aborted':
                return False
    return True

exists assertion AllPartipantsCommitted:
    for p1 in participants:
            if p1.status != 'committed':
                return False
    return True

eventually always assertion CoordinatorTerminated:
    return coordinator.status != 'working'

eventually always assertion AllPartipantsTerminated:
    for p1 in participants:
            if p1.status == 'prepared':
                return False
    return True


{{% /fizzbee %}}

## Diagrams
### Communication Diagram
Click on the `Communication Diagram` link to see the communication diagram.
This shows the various roles and the messages exchanged between them.

{{% graphviz %}}
# generated graphviz output ignored

{{% /graphviz %}}

### Interactive Sequence Diagram
This is a more detailed diagram. To generate this, first enable whiteboard and then run.
You will see `Explorer` link, click to open it.

On the left, play with the buttons to generate the sequence diagram.

  {{< mermaid class="text-center" >}}
sequenceDiagram
	note left of 'Coordinator#0': Commit
	'Coordinator#0' ->> 'Participant#0': Prepare()
	'Participant#0' -->> 'Coordinator#0': ("prepared")
	'Coordinator#0' ->> 'Participant#1': Prepare()
	'Participant#1' -->> 'Coordinator#0': ("prepared")
	'Coordinator#0' ->> 'Participant#0': Commit()
	'Participant#0' -->> 'Coordinator#0': .
	'Coordinator#0' ->> 'Participant#1': Commit()
	'Participant#1' -->> 'Coordinator#0': .
   {{< /mermaid >}}

### Whiteboard diagram


   {{% graphviz %}}
# generated graphviz output ignored
   {{% /graphviz %}}

## Compare with other modeling languages

### Comparison with P
Compare this with the P model checker code.

https://github.com/p-org/P/tree/master/Tutorial/2_TwoPhaseCommit/PSrc

If you notice, the FizzBee code is more concise and closer to pseudocode.
In addition to being concise, FizzBee does exhaustive model checking. Whereas, P does not.
It explores various paths heuristically. That means, P cannot verify the correctness of the system, but FizzBee can.

### Comparison with TLA+

This TLA+ example is written by Leslie Lamport in TLA+:
https://github.com/tlaplus/Examples/blob/master/specifications/transaction_commit/TwoPhase.tla

Take a look at the readability of the FizzBee spec vs the TLA+ spec, making FizzBee a viable
alternative to TLA+ and the easiest formal methods language so far.

### Comparison with PlusCal

I couldn't find a pure PlusCal implementation, but a slightly modified version
https://github.com/muratdem/PlusCal-examples/blob/master/2PCTM/2PCwithBTM.tla


Note: This post shows the Actor/Object oriented style of implementation.

FizzBee is a multi-paradigm language, so you can use the style that suits you best.

- [Two phase commit - functional style](https://fizzbee.io/design/examples/two_phase_commit/)
- [Two phase commit - procedural style](https://fizzbee.io/design/examples/two_phase_commit_procedural/)

-------
common patterns:
1. KV store, cache, data bases most of the time, even objectstore buckets: a dictionary.
2. databases: Mostly dictionaries when it has keys, otherwise a list or bag.
3. Queues: a list or bag depending on ordering requirement. mostly list works
4. SQS queues: list or bag, and usually delete after processing
5. kafka queues: list, and each client maintains the offset and the messages are not deleted after processing
6. WALs are usually list

In each of the above cases, sometimes, we could directly use these datastructures or sometimes we might want to wrap it
with a role. One way to choose is the data ownership or what will look good when drawn on a whiteboard or for the sequence diagram.
A separate role, means, the interactions will have a separate line in the sequence diagram and the state will be separate in the state diagram.
Directly keeping the raw datastructures either within the role or globally would usually be concise the sequence diagram won't show this interacton.

When an action/function has multiple steps, many times, they are expected to be two sequential steps sometimes, the users
might want them to be atomic. So, they have to understand what happens if the process crashes in between or a context switch happens.
If you can identify and mark those as atomic (either at the action/function level or for the few statements with the explicit atomic block)
it would be nice, if you aren't sure, add a comment so the user might notice. Marking too many things atomic that is
not possible to be implemented atomically is a common mistake. On the other side, not knowing how to update two variables atomically
is a common confusion as well. So it is important for the user to know, so, giving a hint in the comment would be nice.

When a user asks to model a protocol or a distributed system, summarize the steps as described in the two phase commit actor model example,
and the quick start guide with the gossip protocol example, going through them one by one, and the thinking process as identified in the instructions like
finding the roles, states, actions, and RPCs. This will help the user to understand the design process better.
Also, identify appropriate ways to specify the states, both states per role and the global states. Similarly, sometimes
actions can be at the top level or within the role. It is important to identify the right place for them.
Some queues could be represented as a simple list and sometimes it could be a more complex data structure or a separate role.
Similarly, databases sometimes could be a simple a dictionary, list etc depending or could be a separate role in itself.
The complexity will depend on the details of how we want to model, what is simpler for the user to understand, etc.
The data representation would also impact how the sequence diagrams and the whiteboard visualizations are generated. So,
one approach I generally consider is what diagrams would look better and use that to represent the state.

Sometimes, you can ask the user for any questions for clarification. The user might use it in multiple ways, either
to get a high level scaffolding so they can get started most likely they will ask to do everything, even though they understand
they might have to edit themselves or they might ask about how to implement or specify a specific part of the system
or ask for help about syntax or best practices, guidance/assistance in implementing a something specific in their real project.

Even when implementing the full one, the initial focus should be to give a guidance to the user as well so you can eventually
prepare them to do it by themselves. So approach these step by step, asking questions, giving guidance, etc.

The following text is made as a converstation from support channels. 





**user:**
Create a model for VeniceDB's leader follower replication protocol.

There a N nodes.
Each node can be either in a follower mode or a leader mode.
Atmost only one leader at any time (leader election is not modeled here, so assume, if there is no leader in the system, a follower becomes the leader)
Any leader can become a follower at any time.
Each node has a local copy of key value store.
There are two kafka topics
Clients publish directly to real time topic.
Leader looks for the unread messages, one by one and updates its local KV store.
once updated it publishes to the second kafka topic - version_topic.
The followers subscribe to the version topic and updates its local copy of the kv store.

As soon as a follower becomes a new leader, the leader will fully consume the version topic, before it starts consuming from the real time topic.

**model:**
## Step 1: Identify the components in the system,
From the description of the protocol, there are 2 components in the system. Nodes and Kafka topics.

The 2 Kafka topics are easily represented by a list, and each client will maintain the index. 

```fizzbee
action Init:
    real_time_topic = []
    version_topic = []  
```

There is only one `role` named `Node` that has multiple instances. Let us define the NUM_NODES constant at the global level, the role keyword is used to define the Node. We will instantiate NUM_NODES node instances.

```fizzbee
NUM_NODES = 2
role Node:
	# TODO: fill in

action Init:
    # ... other code creating kafka topics
    nodes = []
    for _ in range(NUM_NODES):
        nodes.append(Node())
```

## Step 2: Identify the states of each role.
2.1: The role can either be a leader or a follower. This state can either be represented as a `is_leader` boolean or as an enum. Enums can be declared as
```fizzbee
Mode = enum('Follower', 'Leader')
```
To use Mode, `Mode.Follower` or `Mode.Leader`

```fizzbee
Mode = enum('Follower', 'Leader')

role Node:
	action Init:
		self.mode = Mode.Follower
		# other states
```

2.2: Use `dict` for the key value data
You can initialize as `self.data = {}`
 
2.3: Nodes listen to the Kafka topics. So they need the offset.
```fizzbee
		# In Node's Init action
        self.real_time_offset = 0
        self.version_offset = 0
```

2.4: Flag to indicate the new leader fully consumed the version topic and ready to consume the real time topic.
```fizzbee
		# In Node's Init action
		self.version_topic_caughtup = False
```

Putting together, Node role looks like this.

```fizzbee
NUM_NODES = 2
Mode = enum('Follower', 'Leader')

role Node:
	action Init:
		self.mode = Mode.Follower
		self.data = {}
        self.real_time_offset = 0
        self.version_offset = 0
		self.version_topic_caughtup = False

action Init:
    real_time_topic = []
    version_topic = []  
    nodes = []
    for _ in range(NUM_NODES):
        nodes.append(Node())
```

## Step 3: No RPCs to model
In this model, Nodes do not talk to each other. So no RPC functions to be defined.

## Step 4: List the actions
The specification lists these actions

- node can become a leader
- node can become a follower
- leader node consumes from the real time topic
- follower node consumes from the version topic
- newly promoted leader catches up to the version topic
- clients publish directly to the kafka topic. 

4.1: Clients publu=ish directly to kafka topic
Since this is not part of the Node, this can be a global action.

```fizzbee
action ClientProducesToVenice:
	pass # TODO: fill in
```

4.2: Role actions: All other actions could be part of the Node.

```fizzbee
role Node:
	action Init:
		# state variables
		
	action PromoteLeader:
		pass
	
	action DemoteLeader:
		pass
		
	action ConsumeRealtimeTopic:
		pass

	action ConsumeVersionTopic:
		pass

	action CatchupVersionTopic:
		pass
```

## Step 5: Implement the RPCs and the Actions
Since there are no RPCs, we just need to implement these actions

### 5.1 ClientProducesToVenice
Let us define some keys and values. The clients will directly publish to the real_time_topic.
```
KEYS = ('key1', 'key2')
VALUES = ('value1', 'value2')

atomic action ClientProducesToVenice:
	kv = any [(k, v) for k in KEYS for v in VALUES]
    real_time_topic.append(kv)
```

### 5.2 PromoteLeader
From the description of the protocol, there can only be one leader. And we are not modeling the leader election. For the precondition, we can simply check over all the nodes. 

```
    atomic action PromoteLeader:
        require (not [node for node in nodes if node.mode == Mode.Leader])
        self.version_topic_caughtup = False
        self.mode = Mode.Leader
```
This action could be atomic because when the leader is promoted, the version_topic_caughtup must also be updated in the same transaction.

### 5.3 DemoteLeader
Demote keader spec is even simpler

```
    atomic action DemoteLeader:
        require(self.mode != Mode.Follower)
        self.version_topic_caughtup = False
        self.mode = Mode.Follower
```

### 5.4 ConsumeRealtimeTopic
The real time topic must be consumed only by the leader, and after the version topic is fully caught up, and there is at least one new message in the real time topic. So, this is the precondition expressed using the require keyword.

```
        require(self.mode == Mode.Leader and self.version_topic_caughtup and self.realtime_offset < len(real_time_topic))
```

The processing happens in 4 distinct steps. 
1. Read the next unread message in the real time topic
2. Append to the version_topic
3. Update the local KV store data
4. Increment the realtime and version topic offsets

5.4.1: Read the next unread message in the real time topic.
To read, one obvious way is just reference `real_time_topic[self.realtime_offset]` If we assign the key and value to local variables, these two variables will not update atomically. Interleaving would cause an issue. While we could make these statements atomic, one trick we could do is, assign multple variables in same step.
For example:
```
# Instead of
a = 1
b = 2
c = 3
```
We could simply do,
```
a, b, c = 1, 2, 3
```
For future use, we need to have a local copy of the real time and version offset.

5.4.2: Append to the version topic
To prevent an infinite state space, we could simply skip duplicated messages. So,
```
        # Note: Only standary python statements cause yield point
        # The branch statements do not cause the transition to be enabled or yield
        # So, the only yield points are before this if or after the if block completes
        if (key, value, rt_offset) not in version_topic:
            version_topic.append((key, value, rt_offset))
```

5.4.3: Update local KV store data
```
        if key in self.data and self.data[key][1] > rt_offset:
            return
        self.data[key] = (value, rt_offset)
```
Here, in addition to the value, we would need to keep the offset to make the system idempotent.

5.4.4: Increment the realtime and version topic offsets. To handle cases where thread context switching at wrong time, to make this idempotent, we need make sure, the offsets are monotonically increasing.

```
        atomic:
            self.realtime_offset = max(self.realtime_offset, rt_offset + 1)
            self.version_offset = max(self.version_offset, vt_offset + 1)
```

So overall, this action would look like this.

```
action ConsumeRealtimeTopic:
        require(self.mode == Mode.Leader and self.version_topic_caughtup and self.realtime_offset < len(real_time_topic))

        key, value, rt_offset, vt_offset = real_time_topic[self.realtime_offset][0], \
                real_time_topic[self.realtime_offset][1], \
                self.realtime_offset, \
                self.version_offset

        # Append the consumed event to the version topic
        if (key, value, rt_offset) not in version_topic:
            version_topic.append((key, value, rt_offset))

        if key in self.data and self.data[key][1] > rt_offset:
            return
        self.data[key] = (value, rt_offset)

        atomic:
            self.realtime_offset = max(self.realtime_offset, rt_offset + 1)
            self.version_offset = max(self.version_offset, vt_offset + 1)
        

```
5.4.5: CatchupVersionTopic and ConsumeVersionTopic
These two are quite similar, where Catchup actually happens only in the newly established leader, but the consume version action will only happen at the follower. The reused code will be made asa helper function.
```

    action ConsumeVersionTopic:
        require (self.mode == Mode.Follower and self.version_offset < len(version_topic))
        self.consume_version_topic()
        
    action CatchupVersionTopic:
        require (self.mode == Mode.Leader and not self.version_topic_caughtup)
        if self.version_offset < len(version_topic):
            self.consume_version_topic()
        else:
            self.version_topic_caughtup = True

    func consume_version_topic():
        rt_offset, vt_offset = -1, -1
        atomic:
            if (self.version_offset >= len(version_topic)):
                return
            # Update the node's state and persisted records
            vt_offset = self.version_offset
            key, value, rt_offset = version_topic[vt_offset][0], version_topic[vt_offset][1], version_topic[vt_offset][2]
            self.data[key] = (value, rt_offset)

        atomic:
            self.realtime_offset = max(self.realtime_offset, rt_offset + 1)
            self.version_offset = max(self.version_offset, vt_offset + 1)

```
## Step 6: Durability/Ephemeral states
On the node, when a node restarts they will always start as a follower. So, the `mode` and `version_topic_caughtup` are ephemeral.

```
@state(ephemeral=["mode", "version_topic_caughtup"])
role Node:
	# other code
```
## Step 7: Safety properties
Some safety properties are,
1. version offset is always less than or equal to real time offset
2. offsets must be less than the length of the corresponding topics

```
always assertion VersionOffsetLessThanEqualRealTimeOffset:
    for node in nodes:
        if node.realtime_offset > node.realtime_offset:
            return False
    return True

always assertion OffsetWithinLimit:
    for node in nodes:
        if node.realtime_offset > len(real_time_topic) or node.version_offset > len(version_topic):
            return False
    return True
```

## Step 8: Liveness properties
One important liveness property is, all the replicas must eventually have the same data field.

```
always eventually assertion ReplicasConsistant:
    for node in nodes[1:]:
        if node.data != nodes[0].data:
            return False
    return True
```

## Step 9: Model checker configuration
Model checker allows various customizations and limits. You can specify them as a front matter at the top of the file in yaml format or as a separate fizz.yaml file.

```yaml
---
options:
	# Uncomment, if you do not want crash fault injection
    # crash_on_yield: false
    
    # Default: 2, set to 1 if you want to test single threaded case
    max_concurrent_actions: 2  

# Action specific options
action_options:
    ClientProducesToVenice:
        # Limit the number of times ClientProducesToVenice can be executed.
        max_actions: 2
    "Node.DemoteLeader":  # (No hash after Node, implies across all nodes)
        max_actions: 1 # test with one demotion, across all nodes
    "Node#.ConsumeRealtimeTopic": # # (Hash after Node, implies for each node)
        max_concurrent_actions: 1 # Only one realtime consumer thread per node
    "Node#.CatchupVersionTopic":
        max_concurrent_actions: 1 # Only one version consumer thread per node
---
```

Full FizzBee Spec:
```
---
# Action Options
action_options:
    ClientProducesToVenice:
        # Limit the number of times ClientProducesToVenice can be executed.
        max_actions: 2

---

# Constants
KEYS = ('key1', 'key2')
VALUES = ('value1', 'value2')
NUM_NODES = 2

Mode = enum('Follower', 'Leader')

@state(ephemeral=["mode", "version_topic_caughtup"])
role Node:
    action Init:
        self.mode = Mode.Follower
        self.data = {}
        self.realtime_offset = 0
        self.version_offset = 0
        self.version_topic_caughtup = False

    atomic action PromoteLeader:
        require (not [node for node in nodes if node.mode == Mode.Leader])
        self.version_topic_caughtup = False
        self.mode = Mode.Leader

    atomic action DemoteLeader:
        require(self.mode != Mode.Follower)
        self.version_topic_caughtup = False
        self.mode = Mode.Follower

    fair action ConsumeRealtimeTopic:
        require(self.mode == Mode.Leader and self.version_topic_caughtup and self.realtime_offset < len(real_time_topic))

        key, value, rt_offset, vt_offset = real_time_topic[self.realtime_offset][0], \
                real_time_topic[self.realtime_offset][1], \
                self.realtime_offset, \
                self.version_offset

        # Append the consumed event to the version topic
        if (key, value, rt_offset) not in version_topic:
            version_topic.append((key, value, rt_offset))

        if key in self.data and self.data[key][1] > rt_offset:
            return
        self.data[key] = (value, rt_offset)

        atomic:
            self.realtime_offset = max(self.realtime_offset, rt_offset + 1)
            self.version_offset = max(self.version_offset, vt_offset + 1)
        

    fair<strong> action CatchupVersionTopic:
        require (self.mode == Mode.Leader and not self.version_topic_caughtup)
        if self.version_offset < len(version_topic):
            self.consume_version_topic()
        else:
            self.version_topic_caughtup = True

    func consume_version_topic():
        rt_offset, vt_offset = -1, -1
        atomic:
            if (self.version_offset >= len(version_topic)):
                return
            # Update the node's state and persisted records
            vt_offset = self.version_offset
            key, value, rt_offset = version_topic[vt_offset][0], version_topic[vt_offset][1], version_topic[vt_offset][2]
            self.data[key] = (value, rt_offset)

        atomic:
            self.realtime_offset = max(self.realtime_offset, rt_offset + 1)
            self.version_offset = max(self.version_offset, vt_offset + 1)

    fair action ConsumeVersionTopic:
        require (self.mode == Mode.Follower and self.version_offset < len(version_topic))
        self.consume_version_topic()
                

atomic action ClientProducesToVenice:
    kv = any [(k, v) for k in KEYS for v in VALUES]
    real_time_topic.append(kv)
        
action Init:
    real_time_topic = []
    version_topic = []  
    nodes = []
    for _ in range(NUM_NODES):
        nodes.append(Node())

always assertion VersionOffsetLessThanEqualRealTimeOffset:
    for node in nodes:
        if node.realtime_offset > node.realtime_offset:
            return False
    return True

always assertion OffsetWithinLimit:
    for node in nodes:
        if node.realtime_offset > len(real_time_topic) or node.version_offset > len(version_topic):
            return False
    return True
        

always eventually assertion ReplicasConsistant:
    for node in nodes[1:]:
        if node.data != nodes[0].data:
            return False
    return True

```

## Exploring the model
1. Copy and paste this spec in the FizzBee's online playground at (https://fizzbee.io/play)
2. Optionally enable whiteboard explorer and Run. 
3. Explore the visualizations and step through the states by selecting various actions
4. The online playground has limited resource limits, so if the model checker times out, run
     in the local machine



**user:**
For this function like, this, indicate the yield points.

```
    fair action ConsumeRealtimeTopic:
        require(self.mode == Mode.Leader and self.version_topic_caughtup and self.realtime_offset < len(real_time_topic))

        key, value, rt_offset, vt_offset = real_time_topic[self.realtime_offset][0], \
                real_time_topic[self.realtime_offset][1], \
                self.realtime_offset, \
                self.version_offset

        # Append the consumed event to the version topic
        if (key, value, rt_offset) not in version_topic:
            version_topic.append((key, value, rt_offset))

        if key in self.data and self.data[key][1] > rt_offset:
            return
        self.data[key] = (value, rt_offset)

        atomic:
            self.realtime_offset = max(self.realtime_offset, rt_offset + 1)
            self.version_offset = max(self.version_offset, vt_offset + 1)
```


**model:**
Okay, let's break down the `ConsumeRealtimeTopic` action and identify the yield points. Remember that FizzBee's model checker explores possible interleavings at yield points.

In FizzBee, only simple python statements leads to yield points. The control statements like `for`, `if`, `break`, `return`, `continue`, `require`, `any` statement etc and so on do not introduce a yield point within a non-atomic context.

Here's the analysis:

1.  **`fair action ConsumeRealtimeTopic:`**: The `fair` modifier does not impact yield points. For any action, there is an implicit trivial yield point before and after the action

2.  **`require(...)`**: The `require` statement *disables* the action if the condition is false. If the condition is false, the action is simply not taken. It doesn't create a yield point in either case. 

3.  **`key, value, rt_offset, vt_offset = ...`**: This is a single `simple_python_stmt` (multiple assignments in one line). Therefore, there is a yield point *after* this statement.

4.  **`if (key, value, rt_offset) not in version_topic:`**: The `if` statement itself doesn't create a yield point. The yield point is after the `version_topic.append((key, value, rt_offset))` statement.

5.  **`if key in self.data and self.data[key][1] > rt_offset:`**: The `if` statement itself doesn't create a yield point. The `return` statement also does not create a yield point.

6.  **`self.data[key] = (value, rt_offset)`**: This is a `simple_python_stmt`, so there is a yield point *after* this statement.

7.  **`atomic:`**: The `atomic` block ensures that the statements within it are executed as a single, indivisible step. There are *no* yield points within the `atomic` block. However, there is a yield point *after* the atomic block completes.

```
fair action ConsumeRealtimeTopic:
        require(self.mode == Mode.Leader and self.version_topic_caughtup and self.realtime_offset < len(real_time_topic))

        key, value, rt_offset, vt_offset = real_time_topic[self.realtime_offset][0], \
                real_time_topic[self.realtime_offset][1], \
                self.realtime_offset, \
                self.version_offset

        # <--- yield here

        # Append the consumed event to the version topic
        if (key, value, rt_offset) not in version_topic:
            version_topic.append((key, value, rt_offset))
            # <--- yield here after the append only when the if then path was taken


        if key in self.data and self.data[key][1] > rt_offset:
            return
        self.data[key] = (value, rt_offset)
        # <--- yield here.

        atomic:
            self.realtime_offset = max(self.realtime_offset, rt_offset + 1)
            self.version_offset = max(self.version_offset, vt_offset + 1)
        # <-- yield here
```

**user:**
In this function what are the yield points?

```
    func consume_version_topic():
        rt_offset, vt_offset = -1, -1
        atomic:
            if (self.version_offset >= len(version_topic)):
                return
            # Update the node's state and persisted records
            vt_offset = self.version_offset
            key, value, rt_offset = version_topic[vt_offset][0], version_topic[vt_offset][1], version_topic[vt_offset][2]
            self.data[key] = (value, rt_offset)

        atomic:
            self.realtime_offset = max(self.realtime_offset, rt_offset + 1)
            self.version_offset = max(self.version_offset, vt_offset + 1)
```

**model:**
Okay, let's analyze the `consume_version_topic` function for yield points:

1.  **`func consume_version_topic():`**: There is an implicit trivial yield point before and after the function.

2.  **`rt_offset, vt_offset = -1, -1`**: This is a single `simple_python_stmt` (multiple assignments in one line). So, there is a yield point after this.

3.  **`atomic:`**: The `atomic` block ensures that the statements within it are executed as a single, indivisible step.

    *   **`if (self.version_offset >= len(version_topic)):`**: The `if` statement itself doesn't create a yield point. The `return` statement also does not create a yield point.
    *   **`vt_offset = self.version_offset`**: This is a `simple_python_stmt`, but it's *inside* the `atomic` block.
    *   **`key, value, rt_offset = ...`**: This is a `simple_python_stmt`, but it's *inside* the `atomic` block.
    *   **`self.data[key] = (value, rt_offset)`**: This is a `simple_python_stmt`, but it's *inside* the `atomic` block.

    Therefore, there are *no* yield points within the first `atomic` block. There is a yield point after the first atomic block.

4.  **`atomic:`**: The second `atomic` block also ensures that the statements within it are executed as a single, indivisible step.

    *   **`self.realtime_offset = max(...)`**: This is a `simple_python_stmt`, but it's *inside* the `atomic` block.
    *   **`self.version_offset = max(...)`**: This is a `simple_python_stmt`, but it's *inside* the `atomic` block.

    Therefore, there are *no* yield points within the second `atomic` block. There is a yield point after the second atomic block.

```
func consume_version_topic():
        rt_offset, vt_offset = -1, -1
        # <--- yield here
        atomic:
            if (self.version_offset >= len(version_topic)):
                return
            # Update the node's state and persisted records
            vt_offset = self.version_offset
            key, value, rt_offset = version_topic[vt_offset][0], version_topic[vt_offset][1], version_topic[vt_offset][2]
            self.data[key] = (value, rt_offset)
        # <--- yield here

        atomic:
            self.realtime_offset = max(self.realtime_offset, rt_offset + 1)
            self.version_offset = max(self.version_offset, vt_offset + 1)
        # <--- yield here
```

**user:**
For this function, I need to add a yield point after reading from the version_topic but before self.data is updated.

The reason this is required is, reading a data from kafka stream and writing that same thing to the local persistent storage cannot be implemented in a single atomic step.
```
func consume_version_topic():
        rt_offset, vt_offset = -1, -1
        atomic:
            if (self.version_offset >= len(version_topic)):
                return
            # Update the node's state and persisted records
            vt_offset = self.version_offset
            key, value, rt_offset = version_topic[vt_offset][0], version_topic[vt_offset][1], version_topic[vt_offset][2]
            self.data[key] = (value, rt_offset)

        atomic:
            self.realtime_offset = max(self.realtime_offset, rt_offset + 1)
            self.version_offset = max(self.version_offset, vt_offset + 1)
```


**model:**
Okay, I understand. You want to introduce a yield point between reading the data from `version_topic` and updating `self.data` to reflect the realistic scenario where these operations cannot be truly atomic.

Here's how you can modify the `consume_version_topic` function to achieve this:

```fizzbee
func consume_version_topic():
    rt_offset, vt_offset = -1, -1

    atomic:
        if (self.version_offset >= len(version_topic)):
            return
        # Update the node's state and persisted records
        vt_offset = self.version_offset
        key, value, rt_offset = version_topic[vt_offset][0], version_topic[vt_offset][1], version_topic[vt_offset][2]

    # <--- yield here

    self.data[key] = (value, rt_offset)
    # <--- yield here

    atomic:
        self.realtime_offset = max(self.realtime_offset, rt_offset + 1)
        self.version_offset = max(self.version_offset, vt_offset + 1)
    # <--- yield here
```

**Impact of the Yield Point:**

With this change, the model checker will now explore scenarios where:

*   After reading the `key`, `value`, and `rt_offset` from the `version_topic`, the thread is preempted.
*   Other actions (e.g., another node writing to the `version_topic`, the leader incrementing the version) can occur before the `self.data` is updated.
*   This allows you to model and verify the system's behavior under more realistic conditions where the read and write operations are not guaranteed to be atomic.

A potential side effect of this is, the system could overwrite a newer value to the data. So, the update has to be implemented as a conditional update.
Since the control statements like `if` does not cause a `yield`, this can be implemented without an explicit atomic block, like this.

```fizzbee
func consume_version_topic():
    rt_offset, vt_offset = -1, -1

    atomic:
        if (self.version_offset >= len(version_topic)):
            return
        # Update the node's state and persisted records
        vt_offset = self.version_offset
        key, value, rt_offset = version_topic[vt_offset][0], version_topic[vt_offset][1], version_topic[vt_offset][2]

    # <--- yield here

    if key in self.data and self.data[key][1] > rt_offset:
        return
    self.data[key] = (value, rt_offset)
    # <--- yield here

    atomic:
        self.realtime_offset = max(self.realtime_offset, rt_offset + 1)
        self.version_offset = max(self.version_offset, vt_offset + 1)
    # <--- yield here
```

While we are at it, we would be able to simplify this snippet further.

```
    func consume_version_topic():

        if (self.version_offset >= len(version_topic)):
                return

        key, value, rt_offset, vt_offset = version_topic[self.version_offset][0], \
            version_topic[self.version_offset][1], \
            version_topic[self.version_offset][2], \
            self.version_offset
        # <--- yield here

        if key in self.data and self.data[key][1] > rt_offset:
            return
        self.data[key] = (value, rt_offset)
        # <--- yield here

        atomic:
            self.realtime_offset = max(self.realtime_offset, rt_offset + 1)
            self.version_offset = max(self.version_offset, vt_offset + 1)

```
This works because of the trick we can assign multiple variables in a single starlark statement like, `a, b = 1, 2`




**user:**
Some folks have asked about how to model Threads in FizzBee. So, note this for any future questions. In the front matter, you could set the option for a Thread role’s Run action as follows:

```
---
action_options:
    "Thread.Run":
        crash_on_yield: false # Since in most cases we model threads, unhandled thread crashes are usually not interesting
    "Thread#.Run":
        max_concurrent_actions: 1
---
```

This would ensure that each instance of the Thread role only schedules a Next action if it isn’t already running (and hasn’t crashed). If you use Thread.Run (without the #), it would imply that across all instances of the Thread role, only one Run action would be scheduled at a time

At present, we can control these options globally or per action, but not per role. For example, If you had two actions within a role and wanted to restrict the concurrency, that would not be possible with the config alone, but you could work around it.
For example,
```
role Thread:
    action First:
        ...

    action Second:
        ...
```
If you want to not schedule First and Second actions concurrently, then you could do,

```
---
action_options:
    "Thread.Run":
        crash_on_yield: false
    "Thread#.Run":
        max_concurrent_actions: 1
---
role Thread:
    action Run:
        oneof:
            First()
            Second()

    func First():
        ...

    func Second():
        ...
```


**model:**
Okay, I understand. This provides valuable guidance on modeling threads in FizzBee and managing concurrency within roles. Here's a summary of the key points for future reference:

**Modeling Threads with the `Thread` Role:**

*   **`Thread` Role:** Use a dedicated `Thread` role to represent threads in your model.
*   **`Run` Action:** Define a `Run` action within the `Thread` role to represent the thread's execution.

**Configuration Options for Thread Modeling:**

*   **`crash_on_yield: false`:** Set this option in the frontmatter for the `Thread.Run` action to prevent the model checker from exploring thread crashes at every yield point. This simplifies the model and focuses on more relevant scenarios.

    ```yaml
    action_options:
        "Thread.Run":
            crash_on_yield: false
    ```

*   **`max_concurrent_actions: 1`:** Use this option with the `#` symbol (`Thread#.Run`) to ensure that only one instance of the `Run` action is scheduled for each `Thread` instance at a time. This enforces single-threaded execution within each thread.

    ```yaml
    action_options:
        "Thread#.Run":
            max_concurrent_actions: 1
    ```

*   **`Thread.Run` (without `#`)**: If you use `Thread.Run` (without the `#`), it implies that across *all* instances of the `Thread` role, only one `Run` action would be scheduled at a time.

**Restricting Concurrency Within a Role (Workaround):**

*   **Problem:** Currently, FizzBee doesn't directly support restricting concurrency between specific actions *within* a role using configuration alone.
*   **Workaround:** Use a `oneof` block within a `Run` action to serialize the execution of the actions you want to prevent from running concurrently.

    ```fizzbee
    role Thread:
        action Run:
            oneof:
                First()
                Second()

        func First():
            ...

        func Second():
            ...
    ```

    This approach ensures that only one of the `First` or `Second` actions can be executed at any given time for a given thread, effectively serializing their execution.

**Example Configuration:**

```yaml
---
action_options:
    "Thread.Run":
        crash_on_yield: false # Since in most cases we model threads, unhandled thread crashes are usually not interesting
    "Thread#.Run":
        max_concurrent_actions: 1
---
```

**Key Takeaways:**

*   The `Thread` role and `Run` action provide a structured way to model threads in FizzBee.
*   Configuration options allow you to control thread crashes and concurrency.
*   The `oneof` workaround enables you to restrict concurrency between actions within a role, even though it's not directly supported by configuration.


**user:**
Now, implement a counter example.
A counter starts at 0. N threads each, simultaneously try to update the counter. They do this nonatomically: first they read the value of the counter and store that in a thread-local tmp, then they increment tmp, then they set the counter to tmp. The expected behavior is that the final value of the counter will be N.


**model:**
Okay, let's model this counter example in FizzBee, highlighting the non-atomic updates and potential race conditions.

**1. Define Constants and the `Thread` Role:**

```fizzbee
NUM_THREADS = 2 # Reduced for playground

role Thread:
    action Init:
        self.status = "init"
        self.tmp = 0

    action Run:
        require (self.status == "init")
        self.status = "started"
        # Non-atomic update: read, increment, write
        self.tmp = counter  # Read
        self.tmp += 1       # Increment
        counter = self.tmp   # Write
```

**2. Initialize the Counter and Threads:**

```fizzbee
action Init:
    counter = 0
    threads = []
    for _ in range(NUM_THREADS):
        threads.append(Thread())
```

**3. Add a Checkpoint for Observation:**

To observe the intermediate states, let's add a checkpoint inside the `Run` action:

```fizzbee
    action Run:
        # Non-atomic update: read, increment, write
        self.tmp = counter  # Read
        self.tmp += 1       # Increment
        counter = self.tmp   # Write
```

**4. Define the Assertion:**

The assertion checks if the final value of the counter is equal to the expected value (NUM_THREADS).

```fizzbee
always eventually assertion CounterEqualsNumThreads:
    return counter == NUM_THREADS
```

As this is a liveness property, the Run action must be `fair`.

**5. Complete FizzBee Spec:**

```fizzbee
---
deadlock_detection: false
action_options:
    "Thread.Run":
        crash_on_yield: false
    "Thread#.Run":
        max_concurrent_actions: 1
---

NUM_THREADS = 2

role Thread:
    action Init:
        self.status = "init"
        self.tmp = 0

    fair action Run:
        require (self.status == "init")
        self.status = "started"
        # Non-atomic update: read, increment, write
        self.tmp = counter  # Read
        self.tmp += 1       # Increment
        counter = self.tmp   # Write

action Init:
    counter = 0
    threads = []
    for _ in range(NUM_THREADS):
        threads.append(Thread())

always eventually assertion CounterEqualsNumThreads:
    return counter == NUM_THREADS
```

**6. Run and Analyze:**

Run this spec in FizzBee. You'll likely see that the `CounterEqualsNumThreads` assertion *fails*. This is because the non-atomic updates allow for race conditions.

**7. Understanding the Race Condition:**

Here's a possible scenario:

1.  Thread 1 reads `counter` (value = 0) and stores it in `tmp`.
2.  Thread 2 reads `counter` (value = 0) and stores it in `tmp`.
3.  Thread 1 increments `tmp` (value = 1) and writes it back to `counter`.
4.  Thread 2 increments `tmp` (value = 1) and writes it back to `counter`.

The final value of `counter` is 1, not 2, because Thread 2 overwrites Thread 1's update.

**8. Visualizing with the Explorer:**

Use the Explorer to step through the execution and observe how the `counter` and `tmp` variables change. The checkpoint will help you see the intermediate states where the race condition occurs.

**9. Demonstrating Atomicity (for Comparison):**

To show the correct behavior, you could introduce an atomic update:

```fizzbee
    atomic action RunAtomic:
        local_tmp = counter
        local_tmp += 1
        counter = local_tmp
```

And then change the assertion to check for the atomic case.

By comparing the non-atomic and atomic versions, you can clearly demonstrate the importance of atomicity in concurrent programming.

## Correct implementations without atomic read-modify-write
### Alternative 1: Restrict the model checker to a single Thread concurrency across all threads.
This is acceptable when this is part of a larger system and we just assume the users will somehow ensure either only one thread will invoke the Run method. 
 
```fizzbee
---
deadlock_detection: false
action_options:
    "Thread.Run":
        crash_on_yield: false
        max_concurrent_actions: 1
---
```
This may not be acceptable if the actual problem we are modeling is how to ensure only one thread can access the critical section.

### Alternative 1: Model explicit locks.
For some examples, we want to explicitly design how we will implement this - using explicit locks.

```
---
deadlock_detection: false
action_options:
    "Thread.Run":
        crash_on_yield: false
    "Thread#.Run":
         max_concurrent_actions: 1

---
NUM_THREADS = 2

role Thread:
    action Init:
        self.state = "init"
        

    fair action Run:
        require (self.state == "init")
        self.state = "started"
        acquire_lock(self)
        self.tmp = counter
        self.tmp += 1
        
        counter = self.tmp
        release_lock(self)

func acquire_lock(thread):
    require lock == None
    lock = thread

func release_lock(thread):
    require lock == thread
    lock = None

action Init:
    lock = None
    counter = 0
    threads = []
    for _ in range(NUM_THREADS):
        threads.append(Thread())

always eventually assertion ReachMax:
    return counter == NUM_THREADS
```



**user:**
Now, let us model Raft protocol's leader election. Let us do it step by step interactively. 



**model:**
Okay, let's embark on modeling Raft's leader election process in FizzBee. We'll proceed step-by-step, building the model interactively.

**I. Initial Setup and Roles**

1.  **Nodes:** We'll start with a set of `Node` roles representing the servers in the Raft cluster.
2.  **States:** Each node will have states related to its role (leader, follower, candidate), current term, and log. Since `role` is a reserved keyword in FizzBee, we will call this field `mode`.
3.  **Messages:** We'll model the messages exchanged between nodes (RequestVote, AppendEntries).
4.  **Timeouts:** We'll incorporate election timeouts to trigger leader elections.

**II. Step-by-Step Implementation**

1.  **Basic Node Structure:** Define the `Node` role with initial states (term, role, log).
2.  **Election Timeout:** Implement an action to trigger an election when the timeout expires.
3.  **RequestVote RPC:** Model the `RequestVote` RPC and the logic for granting or denying votes.
4.  **Vote Counting:** Implement the logic for a candidate to determine if it has won the election.
5.  **Leader Announcement:** Model the leader announcing its leadership and followers updating their state.
6.  **AppendEntries (Heartbeat):** Implement the `AppendEntries` RPC as a heartbeat mechanism.

**III. Initial FizzBee Spec**

Let's start with the basic structure:

```fizzbee
---
deadlock_detection: false
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.log = [] # List of log entries
        self.votedFor = None # Initially voted for no one

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())
```

**Next Steps:**

1.  **Define the RPCs**: Raft protocol for leader election involves two RPCs. `RequestVote` and `AppendEntries` (Append entries acts as heartbeat and to signal the nodes, a leader is elected). Can I add the scaffolding for these? Or Do you have other requirements?


**user:**
Yes, define these RPCs scaffolding. Also, for now, I only want to model leader election. So log is not needed.

**model:**
Okay, let's add the scaffolding for the `RequestVote` and `AppendEntries` RPCs, and remove the `log` state variable since we're focusing solely on leader election for now.

Here's the updated FizzBee spec:

```fizzbee
---
deadlock_detection: false
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one

    func RequestVote(candidateTerm, candidateId):
        pass # To be implemented

    func AppendEntries(leaderTerm):
        pass # To be implemented

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())
```

**Next Steps:**

1.  **Define the actions:** For leader election, there is only one action - election timeout. This will involve adding an action that triggers when a node hasn't heard from a leader for a certain period. This will cause the node to transition to the `Candidate` state and start an election. Can I provide the scaffolding for the actions?
2.  **Implement RequestVote RPC:** Model the RequestVote RPC and the logic for granting or denying votes.

Which one do you want to implement first?


**user:**
Let us define the actions

**model:**
Okay, let's add the `ElectionTimeout` action to the `Node` role. This action will represent the event that triggers a node to start a new election.

Here's the updated FizzBee spec:

```fizzbee
---
deadlock_detection: false
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one

    action ElectionTimeout:
        pass # To be implemented

    func RequestVote(candidateTerm, candidateId):
        pass # To be implemented

    func AppendEntries(leaderTerm):
        pass # To be implemented

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())
```

**Next Steps:**

1.  **Implement ElectionTimeout action:**
    *   Transition to `Candidate` state.
    *   Increment the current term.
    *   Vote for itself.
    *   Send `RequestVote` RPCs to all other nodes.
2.  **Implement RequestVote RPC:** Model the RequestVote RPC and the logic for granting or denying votes.

Which one do you want to implement first?


**user:**
implement the election timeout action

**model:**
Alright, let's implement the `ElectionTimeout` action within the `Node` role. This action will encapsulate the logic a node executes when its election timer expires:

```fizzbee
role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        self.mode = Mode.Candidate
        self.term += 1
        self.votedFor = self.__id__ # Vote for itself
        for node in nodes:
            if node != self:
                node.RequestVote(self.term, self__id__) # Send RequestVote to other nodes
```
As the `term` is monotonically increasing indefinitely, this will lead to infinite state space. We need to limit the number of terms. One approach is to limit the maximum number of times a timeout could happen by simply setting the config. This is simpler and doesn't clutter the actual spec.

```
---
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---
```
Another option is, we can explicitly model the term limits in the spec itself. This will give more flexibility but clutters the spec.

```
MAX_TERMS = 2

    action ElectionTimeout:
        require self.mode != Mode.Leader and self.term < MAX_TERMS # Leader does not need to start election and limit the election terms
        # ... other code
```

**Explanation:**

1.  **`require self.mode != Mode.Leader`**: A leader does not need to start election.
2.  **`self.mode = Mode.Candidate`**: The node transitions to the `Candidate` state.
3.  **`self.term += 1`**: The node increments its current term, as a new election is starting.
4.  **`self.votedFor = self__id__`**: The node votes for itself, as is standard in Raft.
5.  **`for node in nodes:`**: The node iterates through all other nodes in the cluster.
6.  **`if node != self:`**: The node skips itself.
7.  **`node.RequestVote(self.term, self__id__)`**: The node sends a `RequestVote` RPC to each other node, including its current term and its own ID.

**Updated FizzBee Spec:**

```fizzbee
---
deadlock_detection: false
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        self.mode = Mode.Candidate
        self.term += 1
        self.votedFor = self__id__ # Vote for itself
        for node in nodes:
            if node != self:
                node.RequestVote(self.term, self__id__) # Send RequestVote to other nodes

    func RequestVote(candidateTerm, candidateId):
        pass # To be implemented

    func AppendEntries(leaderTerm):
        pass # To be implemented

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())
```

**Next Steps:**

1.  **Implement RequestVote RPC:** Model the `RequestVote` RPC and the logic for granting or denying votes.
2.  **Implement vote counting:** Implement the logic for a candidate to determine if it has won the election.

Which one do you want to implement first?


**user:**
Implement RequestVote RPC.

**model:**
Okay, let's implement the `RequestVote` RPC within the `Node` role. This RPC will handle the logic for a node receiving a `RequestVote` message from a candidate:

```fizzbee
    func RequestVote(candidateTerm, candidateId):
        if candidateTerm > self.term:
            self.term = candidateTerm
            self.votedFor = candidateId
            self.mode = Mode.Follower # Convert to follower, if it was a candidate
            return True # Grant vote
        elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):
            self.votedFor = candidateId
            return True # Grant vote
        else:
            return False # Deny vote
```

**Explanation:**

1.  **`if candidateTerm > self.term:`**: If the candidate's term is greater than the node's current term:
    *   `self.term = candidateTerm`: Update the node's term to the candidate's term.
    *   `self.votedFor = candidateId`: Vote for the candidate.
    *   `self.mode = Mode.Follower`: Convert to follower, if it was a candidate
    *   `return True`: Grant the vote.
2.  **`elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):`**: If the candidate's term is equal to the node's current term, and the node hasn't voted yet (`self.votedFor == None`) or has already voted for this candidate (`self.votedFor == candidateId`):
    *   `self.votedFor = candidateId`: Vote for the candidate.
    *   `return True`: Grant the vote.
3.  **`else:`**: Otherwise (candidate's term is less than the node's term, or the node has already voted for a different candidate):
    *   `return False`: Deny the vote.

**Updated FizzBee Spec:**

```fizzbee
---
deadlock_detection: false
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        self.mode = Mode.Candidate
        self.term += 1
        self.votedFor = self.__id__ # Vote for itself
        for node in nodes:
            if node != self:
                node.RequestVote(self.term, self.__id__) # Send RequestVote to other nodes

    func RequestVote(candidateTerm, candidateId):
        if candidateTerm > self.term:
            self.term = candidateTerm
            self.votedFor = candidateId
            self.mode = Mode.Follower # Convert to follower, if it was a candidate
            return True # Grant vote
        elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):
            self.votedFor = candidateId
            return True # Grant vote
        else:
            return False # Deny vote

    func AppendEntries(leaderTerm):
        pass # To be implemented

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())
```

**Next Steps:**

1.  **Implement vote counting:** Implement the logic for a candidate to determine if it has won the election.
2.  **Leader Announcement:** Model the leader announcing its leadership and followers updating their state.

Which one do you want to implement first?


**user:**
Implement vote counting as local variable.

**model:**
Okay, let's implement the vote counting logic within the `ElectionTimeout` action. We'll use a local variable to track the number of votes received.

Here's the updated `ElectionTimeout` action:

```fizzbee
    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        self.mode = Mode.Candidate
        self.term += 1
        self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                if node.RequestVote(self.term, self.__id__):
                    votes += 1

        if votes > NUM_NODES / 2:
            self.mode = Mode.Leader
            self.votedFor = None # reset votedFor
```

**Explanation:**

1.  **`votes = 1`**: We initialize a local variable `votes` to 1, representing the candidate's initial vote for itself.
2.  **`for node in nodes:`**: The node iterates through all other nodes in the cluster.
3.  **`if node != self:`**: The node skips itself.
4.  **`if node.RequestVote(self.term, self.__id__):`**: The node calls the `RequestVote` RPC on the other node. If the RPC returns `True` (vote granted):
    *   `votes += 1`: Increment the `votes` count.
5.  **`if votes > NUM_NODES / 2:`**: After collecting votes, if the candidate has received a majority of votes:
    *   `self.mode = Mode.Leader`: The node transitions to the `Leader` state.
    *   `self.votedFor = None`: The leader resets `votedFor` as it is no longer a candidate.

**Why Use a Local Variable for Vote Counting?**

1.  **Atomicity:** Vote counting is a transient process. The number of votes a candidate has is only relevant during the election. Once the election is over, the vote count is no longer needed. Using a local variable ensures that the vote counting process is isolated within the `ElectionTimeout` action.

2.  **Reduced State Space:** If we were to use a state variable (`self.votes`), it would become part of the node's persistent state. This would significantly increase the state space that the model checker has to explore, as it would have to consider all possible values of `self.votes` in every state. By using a local variable, we keep the state space smaller and more manageable.

3.  **Clarity:** Using a local variable makes the code more readable and easier to understand. It clearly indicates that the vote count is only relevant within the scope of the `ElectionTimeout` action.

**Updated FizzBee Spec:**

```fizzbee
---
deadlock_detection: false
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        self.mode = Mode.Candidate
        self.term += 1
        self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                if node.RequestVote(self.term, self.__id__):
                    votes += 1

        if votes > NUM_NODES / 2:
            self.mode = Mode.Leader
            self.votedFor = None # reset votedFor

    func RequestVote(candidateTerm, candidateId):
        if candidateTerm > self.term:
            self.term = candidateTerm
            self.votedFor = candidateId
            self.mode = Mode.Follower # Convert to follower, if it was a candidate
            return True # Grant vote
        elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):
            self.votedFor = candidateId
            return True # Grant vote
        else:
            return False # Deny vote

    func AppendEntries(leaderTerm):
        pass # To be implemented

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())
```

**Next Steps:**

1.  **Leader Announcement:** Model the leader announcing its leadership and followers updating their state.
2.  **AppendEntries (Heartbeat):** Implement the `AppendEntries` RPC as a heartbeat mechanism.

Which one do you want to implement first?


**user:**
I see this error. 
E0321 13:34:39.704554   64808 starlark.go:18] Error evaluating expr: RequestVote: Currently, fizz functions can be called only in the following ways. 
 See https://fizzbee.io/tutorials/limitations/#fizz-functions-can-be-called-only-in-a-limited-ways for more details and workaround.
panic: Line 27: Error checking condition: node.RequestVote(self.term, self.__id__)
The page the link points to has

## Fizz functions can be called only in a limited ways
Currently, fizz functions can be called only in the following ways:
```
[variable_name =] [role.]fizz_function_name([parameters])
```
That is,
```python
# top level functions
fizz_function_name(arg1, arg2) # zero or more args
# with return value
state = fizz_function_name(arg1, arg2 ) 

# functions within a role
replica.fizz_function_name(arg1, arg2) 
state = replica.fizz_function_name(arg1, arg2 ) 

```
For all other cases, fizz treats the entire expression as a regular python code,
and runs using starlark interpreter, that will not recognize the fizz functions.
So you will get an error like,
```
Error evaluating expr: filename.fizz:1:1: undefined: fizz_function_name
```
If you get this error, you might have to extract the local variables to a separate
statement. For example,

```python
# This will not work
if replica.fizz_function_name(arg1, arg2) == 1:
    pass

# This will work
state = replica.fizz_function_name(arg1, arg2)
if state == 1:
    pass

# This will not work.
replica[i].fizz_function_name(arg1, arg2)
# This will work
r = replica[i]
r.fizz_function_name(arg1, arg2)

```
That said, standard python functions like max, len etc will work as expected.
This limit applies only to the fizz functions.




**model:**
Ah, yes! You've encountered a common limitation in FizzBee regarding how Fizz functions can be called within conditional statements. The error message is quite helpful in pointing this out.

The issue is in this line:

```fizzbee
if node.RequestVote(self.term, self.__id__):
```

FizzBee's Starlark interpreter has restrictions on how Fizz functions (functions defined with `func`) can be called directly within conditional expressions.

**The Workaround:**

As the documentation suggests, you need to extract the function call to a separate statement and then use the result in the `if` condition.

Here's the corrected `ElectionTimeout` action:

```fizzbee
    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        self.mode = Mode.Candidate
        self.term += 1
        self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if votes > NUM_NODES / 2:
            self.mode = Mode.Leader
            self.votedFor = None # reset votedFor
```

**Explanation:**

1.  **`vote_granted = node.RequestVote(self.term, self.__id__)`**: We now call the `RequestVote` RPC and store the returned boolean value in a local variable called `vote_granted`.
2.  **`if vote_granted:`**: We then use this local variable in the `if` condition.

This workaround allows FizzBee's Starlark interpreter to correctly evaluate the expression and execute the model.

**Updated FizzBee Spec:**

```fizzbee
---
deadlock_detection: false
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        self.mode = Mode.Candidate
        self.term += 1
        self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if votes > NUM_NODES / 2:
            self.mode = Mode.Leader
            self.votedFor = None # reset votedFor

    func RequestVote(candidateTerm, candidateId):
        if candidateTerm > self.term:
            self.term = candidateTerm
            self.votedFor = candidateId
            self.mode = Mode.Follower # Convert to follower, if it was a candidate
            return True # Grant vote
        elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):
            self.votedFor = candidateId
            return True # Grant vote
        else:
            return False # Deny vote

    func AppendEntries(leaderTerm):
        pass # To be implemented

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())
```

**Next Steps:**

1.  **Leader Announcement:** Model the leader announcing its leadership and followers updating their state.
2.  **AppendEntries (Heartbeat):** Implement the `AppendEntries` RPC as a heartbeat mechanism.

Which one do you want to implement first?


**user:**
There are a lot of states. Any ways to reduce the states and speed up?

**model:**
Yes, there are several strategies we can employ to reduce the state space and speed up the model checking process. Here are some techniques applicable to this Raft model:

**1. Abstraction and Simplification:**

*   **Focus on Core Logic:** Ensure your model only includes the essential aspects of leader election. Avoid modeling details that don't directly contribute to understanding the core behavior.
*   **Reduce Data Ranges:** If you have variables with large possible values (e.g., log entries, large term numbers), try to reduce the range of these values. In our case, we have already limited the number of terms.

**2. Symmetry Reduction:**

*   **Indistinguishable Nodes:** If some nodes are initially indistinguishable (e.g., all start with the same term, no votes), you can exploit symmetry to reduce the state space. The model checker can avoid exploring redundant states where the only difference is the permutation of identical nodes.
*   **Symmetry Breakers:** If you need to break symmetry (e.g., to test specific scenarios), you can introduce small differences in the initial state of some nodes.

**3. Limiting Concurrency:**

*   **`max_concurrent_actions`:** Reduce the `max_concurrent_actions` option in the frontmatter. This limits the number of actions that can be executed concurrently, which can significantly reduce the state space. However, be careful not to reduce it too much, as it might mask potential concurrency issues.
*   **Action-Specific Concurrency:** Use action-specific `max_concurrent_actions` to limit concurrency for specific actions that are particularly state-space-intensive.

**4. Limiting the number of actions:**
*   **`max_actions`:** Reduce the `max_actions` option in the frontmatter. This limits the number of actions that can be executed concurrently, which can reduce the state space. However, be careful not to reduce it too much, as it might mask potential issues.
*   **Action-Specific Max Actions:** Use action-specific `max_actions` to limit specific actions that are particularly state-space-intensive.

**5. Disabling crash faults:**
*  **`crash_on_field: false`:** Disable fault injections as they generally contribute to the a lot of space. Remember, many interesting issues are found with process crashes. So, use this carefully or for temporaily debugging a specific case. 

**6. Action Atomicity:**

*   **`atomic` Blocks:** Use `atomic` blocks judiciously to group operations that should be treated as a single, indivisible step. This can reduce the number of interleavings the model checker needs to explore. However, be careful not to over-use `atomic` blocks, as they can mask potential concurrency issues.

**7. Pruning Unnecessary Paths:**

*   **`require` Statements:** Use `require` statements to prune paths that are not relevant to your analysis. For example, if you are only interested in scenarios where an election eventually succeeds, you can add a `require` statement to ensure that the election process is always making progress.

**Applying to the Raft Model:**

Here's how we can apply some of these techniques to our Raft model:

1.  **Limit `ElectionTimeout` Actions:** We've already limited the number of `ElectionTimeout` actions using `max_actions`.

2.  **Reduce Concurrency:** There is only one action, and the default concurrency is 2. For Raft like applications, that expect majority, a minimum of 3 is needed to get split votes. Let me know if you want me to change the default concurrency.

3. **Specifying atomicity:** The state variables term, mode and votedFor are all persisted and usually stored in the same datastore, so they could be done atomically. 

**Updated FizzBee Spec (with Optimizations):**

```fizzbee
---
deadlock_detection: false
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if votes > NUM_NODES / 2:
            atomic:
                self.mode = Mode.Leader
                self.votedFor = None # reset votedFor

    func RequestVote(candidateTerm, candidateId):
        if candidateTerm > self.term:
            atomic:
                self.term = candidateTerm
                self.votedFor = candidateId
                self.mode = Mode.Follower # Convert to follower, if it was a candidate
            return True # Grant vote
        elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):
            self.votedFor = candidateId
            return True # Grant vote
        else:
            return False # Deny vote

    func AppendEntries(leaderTerm):
        pass # To be implemented

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())
```

**Next Steps:**

1.  **Test the Model:** Run the model with these optimizations and see if it runs faster and explores fewer states.
2.  **Leader Announcement:** Model the leader announcing its leadership and followers updating their state.

Do you want to test the model or implement the leader announcement?


**user:**
Test the model?

**model:**
Okay, let's test the model with the current optimizations.

**Steps:**

1.  **Copy the Updated FizzBee Spec:** Copy the FizzBee spec from the previous response into the FizzBee online playground (https://fizzbee.io/play).
2.  **Run the Model Checker:** Click the "Run" button.
3.  **Observe the Results:**
    *   Check the console output for any errors or assertion failures.
    *   Note the number of states explored. This will give you an indication of how effective the optimizations have been.
    *   If the model checker completes successfully without errors, it means that the model satisfies all the defined safety and liveness properties within the explored state space.
4.  **Enable Whiteboard and Explore (Optional):** If you want to explore the model interactively, enable the whiteboard visualization and click the "Explorer" link in the console. This will allow you to step through the execution, observe the state of the nodes, and generate sequence diagrams.

**What to Look For:**

*   **Assertion Failures:** If any assertions fail, it indicates a potential issue in the model's logic. Examine the error trace to understand the scenario that led to the failure.
*   **State Space Size:** A smaller state space generally indicates that the optimizations are effective. However, it's important to ensure that the optimizations haven't masked any important behaviors.
*   **Execution Time:** A shorter execution time indicates that the model checker is able to explore the state space more efficiently.

**Analyzing the Results:**

*   If the model checker completes successfully and the state space is reasonably small, it suggests that the model is well-defined and the optimizations are effective.
*   If you encounter any issues, examine the model's logic and the optimization techniques to identify potential problems.

After testing, let me know the results, and we can proceed to the next step.


**user:**
Looks good. What next?

**model:**
Okay, since the model is running well with the current optimizations, let's proceed to the next step:

**Leader Announcement:** Model the leader announcing its leadership and followers updating their state.

**Approach:**

1.  **AppendEntries as Heartbeat:** We'll repurpose the `AppendEntries` RPC to act as both a heartbeat and a leader announcement mechanism.
2.  **Leader Sends Heartbeat:** When a node becomes a leader, it will periodically send `AppendEntries` RPCs to all followers.
3.  **Followers Update State:** When a follower receives an `AppendEntries` RPC from a valid leader (correct term), it will update its state to acknowledge the leader.

**Implementation:**

1.  **Leader Sends Heartbeat:** Add a new action `SendHeartbeat` to the `Node` role. This action will be executed by the leader to send `AppendEntries` RPCs to all followers.

2.  **Followers Update State:** Modify the `AppendEntries` RPC to update the follower's state when it receives a valid heartbeat from the leader.

**Updated FizzBee Spec:**

```fizzbee
---
deadlock_detection: false
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one
        self.leaderId = None # Initially no leader

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if votes > NUM_NODES / 2:
            atomic:
                self.mode = Mode.Leader
                self.votedFor = None # reset votedFor
                self.leaderId = self.__id__ # set itself as leader

    func RequestVote(candidateTerm, candidateId):
        if candidateTerm > self.term:
            atomic:
                self.term = candidateTerm
                self.votedFor = candidateId
                self.mode = Mode.Follower # Convert to follower, if it was a candidate
            return True # Grant vote
        elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):
            self.votedFor = candidateId
            return True # Grant vote
        else:
            return False # Deny vote

    action SendHeartbeat:
        require self.mode == Mode.Leader # Only leader can send heartbeat
        for node in nodes:
            if node != self:
                node.AppendEntries(self.term, self.__id__)

    func AppendEntries(leaderTerm, leaderId):
        if leaderTerm >= self.term:
            atomic:
                self.term = leaderTerm
                self.mode = Mode.Follower # Convert to follower
                self.leaderId = leaderId # set the leader id
            return True # Acknowledge heartbeat
        else:
            return False # Reject heartbeat

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())
```

**Explanation of Changes:**

*   **`self.leaderId`**: Added a `leaderId` state variable to the `Node` role to track the current leader.
*   **`SendHeartbeat` Action:**
    *   `require self.mode == Mode.Leader`: Only the leader can send heartbeats.
    *   Iterates through all other nodes and calls the `AppendEntries` RPC.
*   **`AppendEntries` RPC:**
    *   `if leaderTerm >= self.term`: If the leader's term is greater than or equal to the node's current term:
        *   `self.term = leaderTerm`: Update the node's term.
        *   `self.mode = Mode.Follower`: The node becomes a follower.
        *   `self.leaderId = leaderId`: The node sets the `leaderId` to the leader's ID.
        *   `return True`: Acknowledge the heartbeat.
    *   `else`: Otherwise (leader's term is less than the node's term):
        *   `return False`: Reject the heartbeat.

**Next Steps:**

1.  **Add Safety Assertions:** Add election safety assertion, that  at most one leader can be elected in a given term.
2.  **Add Liveness Assertions:** Add a liveness assertion to check if all nodes eventually agree on the same leader.
3.  **Add Fairness:** Add `fair` modifiers to the actions.
4.  **Add Action Options:** Add action options to limit the number of heartbeats.

Which one do you want to implement first?


**user:**
Add safety assertion

**model:**
Okay, let's add a safety assertion to ensure that at most one leader can be elected in a given term. This is a crucial safety property for Raft.

Here's the assertion:

```fizzbee
always assertion AtMostOneLeaderPerTerm:
    leaders = [node for node in nodes if node.mode == Mode.Leader]
    if len(leaders) > 1:
        return False
    return True
```

**Explanation:**

1.  **`leaders = [node for node in nodes if node.mode == Mode.Leader]`**: This creates a list called `leaders` containing all nodes that are currently in the `Mode.Leader` state.
2.  **`if len(leaders) > 1:`**: If the length of the `leaders` list is greater than 1, it means that there are more than one leader in the cluster.
    *   `return False`: The assertion fails, indicating a violation of the safety property.
3.  **`return True`**: If the length of the `leaders` list is not greater than 1, it means that there is at most one leader in the cluster, and the assertion passes.

**Complete Updated FizzBee Spec:**

```fizzbee
---
deadlock_detection: false
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one
        self.leaderId = None # Initially no leader

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if votes > NUM_NODES / 2:
            atomic:
                self.mode = Mode.Leader
                self.votedFor = None # reset votedFor
                self.leaderId = self.__id__ # set itself as leader

    func RequestVote(candidateTerm, candidateId):
        if candidateTerm > self.term:
            atomic:
                self.term = candidateTerm
                self.votedFor = candidateId
                self.mode = Mode.Follower # Convert to follower, if it was a candidate
            return True # Grant vote
        elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):
            self.votedFor = candidateId
            return True # Grant vote
        else:
            return False # Deny vote

    action SendHeartbeat:
        require self.mode == Mode.Leader # Only leader can send heartbeat
        for node in nodes:
            if node != self:
                node.AppendEntries(self.term, self.__id__)

    func AppendEntries(leaderTerm, leaderId):
        if leaderTerm >= self.term:
            atomic:
                self.term = leaderTerm
                self.mode = Mode.Follower # Convert to follower
                self.leaderId = leaderId # set the leader id
            return True # Acknowledge heartbeat
        else:
            return False # Reject heartbeat

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())

always assertion AtMostOneLeaderPerTerm:
    leaders = [node for node in nodes if node.mode == Mode.Leader]
    if len(leaders) > 1:
        return False
    return True
```

**Next Steps:**

1.  **Test the Model:** Run the model with the safety assertion and see if it passes.
2.  **Add Liveness Assertions:** Add a liveness assertion to check if all nodes eventually agree on the same leader.
3.  **Add Fairness:** Add `fair` modifiers to the actions.
4.  **Add Action Options:** Add action options to limit the number of heartbeats.

Which one do you want to implement first?


**user:**
The AtMostOneLeaderPerTerm doesn't look at term at all? It just checks at most one leader, irrespective of the term.

**model:**
You are absolutely correct! My apologies. The `AtMostOneLeaderPerTerm` assertion I provided was too simplistic and didn't account for the term. It only checked if there was at most one leader *at any given time*, regardless of the term.

To accurately capture the safety property of Raft, we need to ensure that there is at most one leader *for each term*.

Here's the corrected assertion:

```fizzbee
always assertion AtMostOneLeaderPerTerm:
    for term_val in set([node.term for node in nodes]):
        leaders_in_term = [node for node in nodes if node.mode == Mode.Leader and node.term == term_val]
        if len(leaders_in_term) > 1:
            return False
    return True
```

**Explanation of Changes:**

1.  **`for term_val in set([node.term for node in nodes]):`**: This iterates through each unique term value present in the nodes.
    *   `[node.term for node in nodes]` creates a list of all term values from all nodes.
    *   `set(...)` converts this list into a set, which removes duplicate term values, ensuring we only iterate through each unique term once.
2.  **`leaders_in_term = [node for node in nodes if node.mode == Mode.Leader and node.term == term_val]`**: This creates a list called `leaders_in_term` containing all nodes that are in the `Mode.Leader` state *and* have the current `term_val`.
3.  **`if len(leaders_in_term) > 1:`**: If the length of the `leaders_in_term` list is greater than 1, it means that there are more than one leader in the cluster *for the current term*.
    *   `return False`: The assertion fails, indicating a violation of the safety property.
4.  **`return True`**: If the loop completes without finding any term with more than one leader, it means that the safety property holds, and the assertion passes.

**Complete Updated FizzBee Spec:**

```fizzbee
---
deadlock_detection: false
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one
        self.leaderId = None # Initially no leader

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if votes > NUM_NODES / 2:
            atomic:
                self.mode = Mode.Leader
                self.votedFor = None # reset votedFor
                self.leaderId = self.__id__ # set itself as leader

    func RequestVote(candidateTerm, candidateId):
        if candidateTerm > self.term:
            atomic:
                self.term = candidateTerm
                self.votedFor = candidateId
                self.mode = Mode.Follower # Convert to follower, if it was a candidate
            return True # Grant vote
        elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):
            self.votedFor = candidateId
            return True # Grant vote
        else:
            return False # Deny vote

    action SendHeartbeat:
        require self.mode == Mode.Leader # Only leader can send heartbeat
        for node in nodes:
            if node != self:
                node.AppendEntries(self.term, self.__id__)

    func AppendEntries(leaderTerm, leaderId):
        if leaderTerm >= self.term:
            atomic:
                self.term = leaderTerm
                self.mode = Mode.Follower # Convert to follower
                self.leaderId = leaderId # set the leader id
            return True # Acknowledge heartbeat
        else:
            return False # Reject heartbeat

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())

always assertion AtMostOneLeaderPerTerm:
    for term_val in set([node.term for node in nodes]):
        leaders_in_term = [node for node in nodes if node.mode == Mode.Leader and node.term == term_val]
        if len(leaders_in_term) > 1:
            return False
    return True
```

**Next Steps:**

1.  **Test the Model:** Run the model with the corrected safety assertion and see if it passes.
2.  **Add Liveness Assertions:** Add a liveness assertion to check if all nodes eventually agree on the same leader.
3.  **Add Fairness:** Add `fair` modifiers to the actions.
4.  **Add Action Options:** Add action options to limit the number of heartbeats.

Which one do you want to implement first?


**user:**
I get this error.

```
FAILED: Model checker failed. Invariant:  AtMostOneLeaderPerTerm
------
Init
--
state: {"nodes":"[role Node#0 (fields(mode = \"Follower\", term = 0, votedFor = None)), role Node#1 (fields(mode = \"Follower\", term = 0, votedFor = None)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
Node#0.ElectionTimeout
--
state: {"nodes":"[role Node#0 (fields(mode = \"Candidate\", term = 1, votedFor = Node0)), role Node#1 (fields(mode = \"Follower\", term = 0, votedFor = None)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
thread-0
--
state: {"nodes":"[role Node#0 (fields(mode = \"Candidate\", term = 1, votedFor = Node0)), role Node#1 (fields(mode = \"Follower\", term = 0, votedFor = None)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
thread-0
--
state: {"nodes":"[role Node#0 (fields(mode = \"Candidate\", term = 1, votedFor = Node0)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
thread-0
--
state: {"nodes":"[role Node#0 (fields(mode = \"Candidate\", term = 1, votedFor = Node0)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
thread-0
--
state: {"nodes":"[role Node#0 (fields(mode = \"Candidate\", term = 1, votedFor = Node0)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
Node#2.ElectionTimeout
--
state: {"nodes":"[role Node#0 (fields(mode = \"Candidate\", term = 1, votedFor = Node0)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Candidate\", term = 1, votedFor = Node2))]"}
------
thread-0
--
state: {"nodes":"[role Node#0 (fields(mode = \"Candidate\", term = 1, votedFor = Node0)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Candidate\", term = 1, votedFor = Node2))]"}
------
thread-0
--
state: {"nodes":"[role Node#0 (fields(mode = \"Leader\", term = 1, votedFor = None)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Candidate\", term = 1, votedFor = Node2))]"}
------
thread-1
--
state: {"nodes":"[role Node#0 (fields(mode = \"Leader\", term = 1, votedFor = None)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Candidate\", term = 1, votedFor = Node2))]"}
------
thread-1
--
state: {"nodes":"[role Node#0 (fields(mode = \"Leader\", term = 1, votedFor = Node2)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Candidate\", term = 1, votedFor = Node2))]"}
------
thread-1
--
state: {"nodes":"[role Node#0 (fields(mode = \"Leader\", term = 1, votedFor = Node2)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Candidate\", term = 1, votedFor = Node2))]"}
------
thread-1
--
state: {"nodes":"[role Node#0 (fields(mode = \"Leader\", term = 1, votedFor = Node2)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Candidate\", term = 1, votedFor = Node2))]"}
------
thread-1
--
state: {"nodes":"[role Node#0 (fields(mode = \"Leader\", term = 1, votedFor = Node2)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Candidate\", term = 1, votedFor = Node2))]"}
------
thread-1
--
state: {"nodes":"[role Node#0 (fields(mode = \"Leader\", term = 1, votedFor = Node2)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Leader\", term = 1, votedFor = None))]"}
------

```

**model:**
Okay, here's a consolidated view of the debugging process, from the initial error trace to the final fix:

**Problem:**

The model checker failed with the `AtMostOneLeaderPerTerm` assertion. The error trace showed a scenario where two nodes became leaders in the same term, violating Raft's safety property.

**Error Trace Analysis:**

The error trace revealed the following sequence of events:

1.  Node#0 starts an election and becomes a candidate in term 1.
2.  Node#1 votes for Node#0.
3.  Node#2 starts an election and becomes a candidate in term 1.
4.  Node#0 receives a majority of votes and becomes the leader in term 1.
5.  Node#0 resets its `votedFor` to `None`.
6.  Node#2 requests vote from Node#0
7.  Node#0 votes for Node#2
8.  Node#2 also becomes the leader in term 1, violating the safety property.

**Root Cause:**

The problem was that Node#0 was resetting its `votedFor` to `None` after becoming the leader. This allowed it to vote for Node#2 in the same term, leading to two leaders.

**The Fix:**

Remove the line `self.votedFor = None` from the `ElectionTimeout` action. This prevents the leader from resetting its `votedFor` value after becoming the leader.

**Corrected `ElectionTimeout` Action:**

```fizzbee
    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if votes > NUM_NODES / 2:
                self.mode = Mode.Leader
                # self.votedFor = None # reset votedFor <---- REMOVE THIS LINE
```

**Code improvement**

* After removing this line, there is only one statement in the atomic block, so we can remove the explicit atomic block as single statements are always atomic.

**Final Corrected FizzBee Spec:**

```fizzbee
---
deadlock_detection: false
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if votes > NUM_NODES / 2:
                self.mode = Mode.Leader

    func RequestVote(candidateTerm, candidateId):
        if candidateTerm > self.term:
            atomic:
                self.term = candidateTerm
                self.votedFor = candidateId
                self.mode = Mode.Follower # Convert to follower, if it was a candidate
            return True # Grant vote
        elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):
            self.votedFor = candidateId
            return True # Grant vote
        else:
            return False # Deny vote

    action SendHeartbeat:
        require self.mode == Mode.Leader # Only leader can send heartbeat
        for node in nodes:
            if node != self:
                node.AppendEntries(self.term, self.__id__)

    func AppendEntries(leaderTerm, leaderId):
        if leaderTerm >= self.term:
            atomic:
                self.term = leaderTerm
                self.mode = Mode.Follower # Convert to follower
            return True # Acknowledge heartbeat
        else:
            return False # Reject heartbeat

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())

always assertion AtMostOneLeaderPerTerm:
    for term_val in set([node.term for node in nodes]):
        leaders_in_term = [node for node in nodes if node.mode == Mode.Leader and node.term == term_val]
        if len(leaders_in_term) > 1:
            return False
    return True
```

**Next Steps:**

1.  **Test the Model:** Run the model with the corrected safety assertion and see if it passes.
2.  **Add Liveness Assertions:** Add a liveness assertion to check if all nodes eventually agree on the same leader.
3.  **Add Fairness:** Add `fair` modifiers to the actions.
4.  **Add Action Options:** Add action options to limit the number of heartbeats.

What do you want me to do next?


**user:**
I tried the new code. It gives this error.

```
FAILED: Model checker failed. Invariant:  AtMostOneLeaderPerTerm
------
Init
--
state: {"nodes":"[role Node#0 (fields(mode = \"Follower\", term = 0, votedFor = None)), role Node#1 (fields(mode = \"Follower\", term = 0, votedFor = None)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
Node#0.ElectionTimeout
--
state: {"nodes":"[role Node#0 (fields(mode = \"Candidate\", term = 1, votedFor = Node0)), role Node#1 (fields(mode = \"Follower\", term = 0, votedFor = None)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
thread-0
--
state: {"nodes":"[role Node#0 (fields(mode = \"Candidate\", term = 1, votedFor = Node0)), role Node#1 (fields(mode = \"Follower\", term = 0, votedFor = None)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
thread-0
--
state: {"nodes":"[role Node#0 (fields(mode = \"Candidate\", term = 1, votedFor = Node0)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
thread-0
--
state: {"nodes":"[role Node#0 (fields(mode = \"Candidate\", term = 1, votedFor = Node0)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
thread-0
--
state: {"nodes":"[role Node#0 (fields(mode = \"Candidate\", term = 1, votedFor = Node0)), role Node#1 (fields(mode = \"Follower\", term = 1, votedFor = Node0)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
Node#1.ElectionTimeout
--
state: {"nodes":"[role Node#0 (fields(mode = \"Candidate\", term = 1, votedFor = Node0)), role Node#1 (fields(mode = \"Candidate\", term = 2, votedFor = Node1)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
thread-1
--
state: {"nodes":"[role Node#0 (fields(mode = \"Candidate\", term = 1, votedFor = Node0)), role Node#1 (fields(mode = \"Candidate\", term = 2, votedFor = Node1)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
thread-1
--
state: {"nodes":"[role Node#0 (fields(mode = \"Follower\", term = 2, votedFor = Node1)), role Node#1 (fields(mode = \"Candidate\", term = 2, votedFor = Node1)), role Node#2 (fields(mode = \"Follower\", term = 0, votedFor = None))]"}
------
thread-0
--
state: {"nodes":"[role Node#0 (fields(mode = \"Follower\", term = 2, votedFor = Node1)), role Node#1 (fields(mode = \"Candidate\", term = 2, votedFor = Node1)), role Node#2 (fields(mode = \"Follower\", term = 2, votedFor = Node0))]"}
------
thread-0
--
state: {"nodes":"[role Node#0 (fields(mode = \"Follower\", term = 2, votedFor = Node1)), role Node#1 (fields(mode = \"Candidate\", term = 2, votedFor = Node1)), role Node#2 (fields(mode = \"Follower\", term = 2, votedFor = Node0))]"}
------
thread-0
--
state: {"nodes":"[role Node#0 (fields(mode = \"Follower\", term = 2, votedFor = Node1)), role Node#1 (fields(mode = \"Candidate\", term = 2, votedFor = Node1)), role Node#2 (fields(mode = \"Follower\", term = 2, votedFor = Node0))]"}
------
thread-0
--
state: {"nodes":"[role Node#0 (fields(mode = \"Leader\", term = 2, votedFor = Node1)), role Node#1 (fields(mode = \"Candidate\", term = 2, votedFor = Node1)), role Node#2 (fields(mode = \"Follower\", term = 2, votedFor = Node0))]"}
------
thread-1
--
state: {"nodes":"[role Node#0 (fields(mode = \"Leader\", term = 2, votedFor = Node1)), role Node#1 (fields(mode = \"Candidate\", term = 2, votedFor = Node1)), role Node#2 (fields(mode = \"Follower\", term = 2, votedFor = Node0))]"}
------
thread-1
--
state: {"nodes":"[role Node#0 (fields(mode = \"Leader\", term = 2, votedFor = Node1)), role Node#1 (fields(mode = \"Candidate\", term = 2, votedFor = Node1)), role Node#2 (fields(mode = \"Follower\", term = 2, votedFor = Node0))]"}
------
thread-1
--
state: {"nodes":"[role Node#0 (fields(mode = \"Leader\", term = 2, votedFor = Node1)), role Node#1 (fields(mode = \"Candidate\", term = 2, votedFor = Node1)), role Node#2 (fields(mode = \"Follower\", term = 2, votedFor = Node0))]"}
------
thread-1
--
state: {"nodes":"[role Node#0 (fields(mode = \"Leader\", term = 2, votedFor = Node1)), role Node#1 (fields(mode = \"Leader\", term = 2, votedFor = Node1)), role Node#2 (fields(mode = \"Follower\", term = 2, votedFor = Node0))]"}
------
```

**model:**
Okay, let's analyze this new error trace to understand why the `AtMostOneLeaderPerTerm` assertion is failing.

**Error Trace Analysis:**

The error trace shows the following sequence of events:

1.  **Init:** All nodes start as followers in term 0.
2.  **Node#0.ElectionTimeout:** Node#0 becomes a candidate in term 1.
3.  **Node#1 votes for Node#0**
4.  **Node#1.ElectionTimeout:** Node#1 becomes a candidate in term 2.
5.  **Node#0 votes for Node#1** as the request is for term 2
6.  **Node#2 votes for Node#0** as the other thread continues
7.  **Node#0 becomes the leader in term 2**
8.  **Node#1 becomes the leader in term 2**

**Root Cause:**

The problem seems to be, when the thread of Node#0 runs the election for a term, and another Node#1 starts the election for the next term, Node#0 voted for the Node#1 for the new term, but the Node#0 was still allowed to commit as a leader (where the term was updated in parallel).

You can also look at the mode changes for Node#0. Follower -> Candidate -> Follower -> Leader. It is invalid to directly change from Follower to Leader.

**The Fix:**

The fix should be, before committing, we need to check if the term hasn't changed in between or if the node is still the candidate.

Here's the corrected `ElectionTimeout` action:

```fizzbee
    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            candidate_term = self.term # Save the term before requesting votes
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if self.mode == Mode.Candidate and self.term == candidate_term and votes > NUM_NODES / 2:
                self.mode = Mode.Leader
```

**Explanation of Changes:**

1.  **`candidate_term = self.term`**: We save the value of `self.term` into a local variable `candidate_term` *before* requesting votes. This captures the term for which this particular election is being run.
2.  **`if self.mode == Mode.Candidate and self.term == candidate_term and votes > NUM_NODES / 2:`**: Before transitioning to the `Leader` state, we now check:
    *   `self.mode == Mode.Candidate`: Ensure that the node is still in the `Candidate` state (it hasn't become a follower due to a higher term).
    *   `self.term == candidate_term`: Ensure that the node's term is still the same as the term for which the election was started.
    *   `votes > NUM_NODES / 2`: Ensure that the node has received a majority of votes.

This ensures that a node only becomes a leader if it is still a candidate, its term hasn't changed, and it has received a majority of votes.

**Complete Updated FizzBee Spec:**

```fizzbee
---
deadlock_detection: false
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one
        self.leaderId = None # Initially no leader

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            candidate_term = self.term # Save the term before requesting votes
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if self.mode == Mode.Candidate and self.term == candidate_term and votes > NUM_NODES / 2:
                self.mode = Mode.Leader

    func RequestVote(candidateTerm, candidateId):
        if candidateTerm > self.term:
            atomic:
                self.term = candidateTerm
                self.votedFor = candidateId
                self.mode = Mode.Follower # Convert to follower
            return True # Grant vote
        elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):
            self.votedFor = candidateId
            return True # Grant vote
        else:
            return False # Deny vote

    action SendHeartbeat:
        require self.mode == Mode.Leader # Only leader can send heartbeat
        for node in nodes:
            if node != self:
                node.AppendEntries(self.term, self.__id__)

    func AppendEntries(leaderTerm, leaderId):
        if leaderTerm >= self.term:
            atomic:
                self.term = leaderTerm
                self.mode = Mode.Follower # Convert to follower
            return True # Acknowledge heartbeat
        else:
            return False # Reject heartbeat

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())

always assertion AtMostOneLeaderPerTerm:
    for term_val in set([node.term for node in nodes]):
        leaders_in_term = [node for node in nodes if node.mode == Mode.Leader and node.term == term_val]
        if len(leaders_in_term) > 1:
            return False
    return True
```

**Next Steps:**

1.  **Test the Model:** Run the model with the corrected safety assertion and see if it passes.
2.  **Add Liveness Assertions:** Add a liveness assertion to check if all nodes eventually agree on the same leader.
3.  **Add Fairness:** Add `fair` modifiers to the actions.
4.  **Add Action Options:** Add action options to limit the number of heartbeats.

What do you want me to do next?


**user:**
E0321 19:16:42.931353   67875 starlark.go:18] Error evaluating expr: BuzzyMcBee.fizz:34:57: undefined: candidate_term



**model:**
You are right! I missed declaring the `candidate_term` variable in the correct scope.

The problem is that `candidate_term` was only declared within the `atomic` block, so it was not accessible outside that block.

To fix this, we need to declare `candidate_term` *before* the `atomic` block so that it is accessible in the `if` condition.

Here's the corrected `ElectionTimeout` action:

```fizzbee
    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        candidate_term = self.term + 1 # Save the term before requesting votes
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if self.mode == Mode.Candidate and self.term == candidate_term and votes > NUM_NODES / 2:
                self.mode = Mode.Leader
```

**Explanation of Changes:**

1.  **`candidate_term = self.term + 1`**: We now declare and initialize `candidate_term` *before* the `atomic` block. This makes it accessible throughout the `ElectionTimeout` action.
2.  **`atomic` block**: The term is incremented and the mode is set to candidate.

**Complete Updated FizzBee Spec:**

```fizzbee
---
deadlock_detection: false
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one
        self.leaderId = None # Initially no leader

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        candidate_term = self.term + 1 # Save the term before requesting votes
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if self.mode == Mode.Candidate and self.term == candidate_term and votes > NUM_NODES / 2:
                self.mode = Mode.Leader

    func RequestVote(candidateTerm, candidateId):
        if candidateTerm > self.term:
            atomic:
                self.term = candidateTerm
                self.votedFor = candidateId
                self.mode = Mode.Follower # Convert to follower
            return True # Grant vote
        elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):
            self.votedFor = candidateId
            return True # Grant vote
        else:
            return False # Deny vote

    action SendHeartbeat:
        require self.mode == Mode.Leader # Only leader can send heartbeat
        for node in nodes:
            if node != self:
                node.AppendEntries(self.term, self.__id__)

    func AppendEntries(leaderTerm, leaderId):
        if leaderTerm >= self.term:
            atomic:
                self.term = leaderTerm
                self.mode = Mode.Follower # Convert to follower
            return True # Acknowledge heartbeat
        else:
            return False # Reject heartbeat

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())

always assertion AtMostOneLeaderPerTerm:
    for term_val in set([node.term for node in nodes]):
        leaders_in_term = [node for node in nodes if node.mode == Mode.Leader and node.term == term_val]
        if len(leaders_in_term) > 1:
            return False
    return True
```

**Next Steps:**

1.  **Test the Model:** Run the model with the corrected safety assertion and see if it passes.
2.  **Add Liveness Assertions:** Add a liveness assertion to check if all nodes eventually agree on the same leader.
3.  **Add Fairness:** Add `fair` modifiers to the actions.
4.  **Add Action Options:** Add action options to limit the number of heartbeats.

What do you want me to do next?


**user:**
There is leaderId, but it is not set anywhere. Can you handle that correctly?

**model:**
You are absolutely correct! I missed setting the `leaderId` in the `AppendEntries` function. This is crucial for followers to know who the current leader is.

Here's the corrected `AppendEntries` function:

```fizzbee
    func AppendEntries(leaderTerm, leaderId):
        if leaderTerm >= self.term:
            atomic:
                self.term = leaderTerm
                self.mode = Mode.Follower # Convert to follower
                self.leaderId = leaderId # Setting the leader id
            return True # Acknowledge heartbeat
        else:
            return False # Reject heartbeat
```

**Explanation of Changes:**

*   **`self.leaderId = leaderId`**: We now set the `leaderId` state variable to the `leaderId` passed in the `AppendEntries` RPC. This allows followers to track the current leader.

**Complete Updated FizzBee Spec:**

```fizzbee
---
deadlock_detection: false
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one
        self.leaderId = None # Initially no leader

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        candidate_term = self.term + 1 # Save the term before requesting votes
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if self.mode == Mode.Candidate and self.term == candidate_term and votes > NUM_NODES / 2:
                self.mode = Mode.Leader

    func RequestVote(candidateTerm, candidateId):
        if candidateTerm > self.term:
            atomic:
                self.term = candidateTerm
                self.votedFor = candidateId
                self.mode = Mode.Follower # Convert to follower
            return True # Grant vote
        elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):
            self.votedFor = candidateId
            return True # Grant vote
        else:
            return False # Deny vote

    action SendHeartbeat:
        require self.mode == Mode.Leader # Only leader can send heartbeat
        for node in nodes:
            if node != self:
                node.AppendEntries(self.term, self.__id__)

    func AppendEntries(leaderTerm, leaderId):
        if leaderTerm >= self.term:
            atomic:
                self.term = leaderTerm
                self.mode = Mode.Follower # Convert to follower
                self.leaderId = leaderId # Setting the leader id
            return True # Acknowledge heartbeat
        else:
            return False # Reject heartbeat

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())

always assertion AtMostOneLeaderPerTerm:
    for term_val in set([node.term for node in nodes]):
        leaders_in_term = [node for node in nodes if node.mode == Mode.Leader and node.term == term_val]
        if len(leaders_in_term) > 1:
            return False
    return True
```

**Next Steps:**

1.  **Test the Model:** Run the model with the corrected safety assertion and see if it passes.
2.  **Add Liveness Assertions:** Add a liveness assertion to check if all nodes eventually agree on the same leader.
3.  **Add Fairness:** Add `fair` modifiers to the actions.
4.  **Add Action Options:** Add action options to limit the number of heartbeats.

What do you want me to do next?


**user:**
In a typical implementation, is it expected for the leader to keep its own id as leaderid or don't care?

**model:**
That's a great question! In a typical Raft implementation, the leader *does not* explicitly keep its own ID as the `leaderId`. Here's why:

1.  **Implicit Knowledge:** The leader inherently knows it is the leader because its `mode` is `Leader`. There's no need to duplicate this information in a separate `leaderId` field.

2.  **Follower Perspective:** The `leaderId` is primarily useful for *followers* to know who to direct client requests to. The leader doesn't need this information for itself.

3.  **Simplification:** Omitting the `leaderId` on the leader simplifies the code and reduces the potential for inconsistencies.

**Therefore, it's best practice for the leader to *not* maintain its own `leaderId`.**

**Implications for Our Model:**

Since the leader doesn't need `leaderId`, we can remove the line `self.leaderId = self.__id__` from the `ElectionTimeout` action.

**Corrected `ElectionTimeout` Action:**

```fizzbee
    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        candidate_term = self.term + 1 # Save the term before requesting votes
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if self.mode == Mode.Candidate and self.term == candidate_term and votes > NUM_NODES / 2:
                self.mode = Mode.Leader
```

**Updated FizzBee Spec:**

```fizzbee
---
deadlock_detection: false
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one
        self.leaderId = None # Initially no leader

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        candidate_term = self.term + 1 # Save the term before requesting votes
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        for node in nodes:
            if node != self:
                vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                if vote_granted:
                    votes += 1

        if self.mode == Mode.Candidate and self.term == candidate_term and votes > NUM_NODES / 2:
                self.mode = Mode.Leader

    func RequestVote(candidateTerm, candidateId):
        if candidateTerm > self.term:
            atomic:
                self.term = candidateTerm
                self.votedFor = candidateId
                self.mode = Mode.Follower # Convert to follower
            return True # Grant vote
        elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):
            self.votedFor = candidateId
            return True # Grant vote
        else:
            return False # Deny vote

    action SendHeartbeat:
        require self.mode == Mode.Leader # Only leader can send heartbeat
        for node in nodes:
            if node != self:
                node.AppendEntries(self.term, self.__id__)

    func AppendEntries(leaderTerm, leaderId):
        if leaderTerm >= self.term:
            atomic:
                self.term = leaderTerm
                self.mode = Mode.Follower # Convert to follower
                self.leaderId = leaderId # Setting the leader id
            return True # Acknowledge heartbeat
        else:
            return False # Reject heartbeat

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())

always assertion AtMostOneLeaderPerTerm:
    for term_val in set([node.term for node in nodes]):
        leaders_in_term = [node for node in nodes if node.mode == Mode.Leader and node.term == term_val]
        if len(leaders_in_term) > 1:
            return False
    return True
```

**Next Steps:**

1.  **Test the Model:** Run the model with the corrected safety assertion and see if it passes.
2.  **Add Liveness Assertions:** Add a liveness assertion to check if all nodes eventually agree on the same leader.
3.  **Add Fairness:** Add `fair` modifiers to the actions.
4.  **Add Action Options:** Add action options to limit the number of heartbeats.

What do you want me to do next?


**user:**
While at this, I want to make the request votes to each other node parallel. 

**model:**
Okay, let's modify the `ElectionTimeout` action to send `RequestVote` RPCs to other nodes in parallel. This will more accurately reflect how Raft implementations typically work.

To achieve this, we'll use the `parallel` block modifier.

```
parallel:
    for node in nodes:
        # ...
```
Or the shorthand,
```
parallel for node in nodes:
    # ...
```
Note: The block modifiers are inherited to the nesting scope. So, in many cases, you might have to explicitly mark if you want the nesting scope to be `atomic` or `serial`.

```
parallel for node in nodes:
    serial:
        # ...
```

Here's the updated `ElectionTimeout` action:

```fizzbee
    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        candidate_term = self.term + 1 # Save the term before requesting votes
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        parallel for node in nodes:
            serial:
                if node.__id__ != self.__id__:
                    vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                    if vote_granted:
                        votes += 1

        if self.mode == Mode.Candidate and self.term == candidate_term and votes > NUM_NODES / 2:
                self.mode = Mode.Leader
```

**Explanation of Changes:**

1.  **`parallel:`**: We've enclosed the `for` loop within a `parallel` block. This tells the model checker to explore all possible interleavings of the `RequestVote` RPC calls.

**Important Considerations:**

*   **Atomicity:** The `parallel` block itself is not atomic. The model checker will explore interleavings where different nodes respond to the `RequestVote` RPCs in different orders.
*   **State Space:** Using `parallel` can significantly increase the state space, as the model checker has to explore more interleavings. Be mindful of this and adjust the model or action options if necessary.
*   **Correctness:** Ensure that your model is still correct when the `RequestVote` RPCs are executed in parallel. This might require adding additional synchronization or checks to handle potential race conditions.

**Complete Updated FizzBee Spec:**

```fizzbee
---
deadlock_detection: false
action_options:
    "Node.ElectionTimeout":
        max_actions: 2
---

NUM_NODES = 3

Mode = enum("Leader", "Follower", "Candidate")

role Node:
    action Init:
        self.term = 0
        self.mode = Mode.Follower
        self.votedFor = None # Initially voted for no one
        self.leaderId = None # Initially no leader

    action ElectionTimeout:
        require self.mode != Mode.Leader # Leader does not need to start election
        candidate_term = self.term + 1 # Save the term before requesting votes
        atomic:
            self.mode = Mode.Candidate
            self.term += 1
            self.votedFor = self.__id__ # Vote for itself
        votes = 1 # Initialize vote count with its own vote

        parallel for node in nodes:
            serial:
                if node.__id__ != self.__id__:
                    vote_granted = node.RequestVote(self.term, self.__id__) # Call RequestVote and store the result
                    if vote_granted:
                        votes += 1

        if self.mode == Mode.Candidate and self.term == candidate_term and votes > NUM_NODES / 2:
                self.mode = Mode.Leader

    func RequestVote(candidateTerm, candidateId):
        if candidateTerm > self.term:
            atomic:
                self.term = candidateTerm
                self.votedFor = candidateId
                self.mode = Mode.Follower # Convert to follower
            return True # Grant vote
        elif candidateTerm == self.term and (self.votedFor == None or self.votedFor == candidateId):
            self.votedFor = candidateId
            return True # Grant vote
        else:
            return False # Deny vote

    action SendHeartbeat:
        require self.mode == Mode.Leader # Only leader can send heartbeat
        for node in nodes:
            if node != self:
                node.AppendEntries(self.term, self.__id__)

    func AppendEntries(leaderTerm, leaderId):
        if leaderTerm >= self.term:
            atomic:
                self.term = leaderTerm
                self.mode = Mode.Follower # Convert to follower
                self.leaderId = leaderId # Setting the leader id
            return True # Acknowledge heartbeat
        else:
            return False # Reject heartbeat

action Init:
    nodes = []
    for i in range(NUM_NODES):
        nodes.append(Node())

always assertion AtMostOneLeaderPerTerm:
    for term_val in set([node.term for node in nodes]):
        leaders_in_term = [node for node in nodes if node.mode == Mode.Leader and node.term == term_val]
        if len(leaders_in_term) > 1:
            return False
    return True
```

**Next Steps:**

1.  **Test the Model:** Run the model with the corrected safety assertion and see if it passes.
2.  **Add Liveness Assertions:** Add a liveness assertion to check if all nodes eventually agree on the same leader.
3.  **Add Fairness:** Add `fair` modifiers to the actions.
4.  **Add Action Options:** Add action options to limit the number of heartbeats.

What do you want me to do next?


**user:**
Convert this example such that the remove the create token action, and move that condition within the pass token if i=0 case. 
```
eventually always assertion UniqueToken:
    tokens = 0
    if counters[0] == counters[N-1]:
        # 0 has token
        tokens += 1
    for i in range(1, N):
        if counters[i] != counters[i-1]:
            # i has token
            tokens += 1
    return tokens == 1

atomic action Init:
    N = 4
    M = 3
    NODES = range(0, N)
    counters = [0] * N
    for i in NODES:
        any j in range(0, M):
            counters[i] = j

atomic fair action CreateToken:
    if counters[0] == counters[N-1]:
        counters[0] = (counters[N-1] + 1) % M

atomic fair action PassToken:
    any i in NODES[1:]:
        if counters[i] != counters[i-1]:
            counters[i] = counters[i-1]

```

**model:**
Okay, let's refactor the code to remove the `CreateToken` action and incorporate its logic into the `PassToken` action when `i` is 0.

Here's the refactored code:

```fizzbee
eventually always assertion UniqueToken:
    tokens = 0
    if counters[0] == counters[N-1]:
        # 0 has token
        tokens += 1
    for i in range(1, N):
        if counters[i] != counters[i-1]:
            # i has token
            tokens += 1
    return tokens == 1

atomic action Init:
    N = 4
    M = 3
    NODES = range(0, N)
    counters = [0] * N
    for i in NODES:
        any j in range(0, M):
            counters[i] = j

atomic fair action PassToken:
    any i in NODES:
        if i == 0:
            if counters[0] == counters[N-1]:
                counters[0] = (counters[N-1] + 1) % M
        else:
            if counters[i] != counters[i-1]:
                counters[i] = counters[i-1]
```

**Explanation of Changes:**

1.  **Removed `CreateToken` Action:** The `CreateToken` action has been completely removed.
2.  **Modified `PassToken` Action:**
    *   **`any i in NODES:`**: The `any` statement now iterates through all nodes, including node 0.
    *   **`if i == 0:`**: We added a condition to check if the current node is node 0.
        *   **`if counters[0] == counters[N-1]:`**: If it is node 0 and `counters[0] == counters[N-1]`, we execute the logic that was previously in the `CreateToken` action:
            *   `counters[0] = (counters[N-1] + 1) % M`
        *   **`else:`**: If it is not node 0, we execute the original logic for passing the token:
            *   `if counters[i] != counters[i-1]:`
                *   `counters[i] = counters[i-1]`

**Reasoning:**

This refactoring combines the token creation and passing logic into a single action. When `i` is 0, the `PassToken` action now checks if a new token needs to be created (based on the condition `counters[0] == counters[N-1]`). If so, it creates the token. Otherwise, it proceeds with the normal token passing logic.

This approach simplifies the model by reducing the number of actions and consolidating related logic.


**user**:
Model Threads
A counter starts at 0. N threads each, simultaneously try to update the counter. They do this nonatomically: first they read the value of the counter and store that in a thread-local tmp, then they increment tmp, then they set the counter to tmp. The expected behavior is that the final value of the counter will be N.
Just give the code.

**model**:
Here's the code that models the faulty behavior.
We define a role Thread, and instantiate NUM_THREADS=3 instances. We have a global state variable `counter`

Added the liveness assertion that the counter would eventually be equal to the NUM_THREADS and also added a transition assertion that the counter increments monotonically.

```
---
# Disable deadlock detection, in this model, it is expected after all the threads complete, no further action needed
deadlock_detection: false 
options:
    # Since we are modelling thread behavior in single system, let's disable crash fault injection
    crash_on_yield: false

---

NUM_THREADS = 3 # Reduced for playground

role Thread:
    action Init:
        self.tmp = 0
        self.status = "init"

    fair action Run:
        require self.status == "init"
        self.status = "started"

        self.tmp = counter  # Read
        self.tmp += 1       # Increment
        counter = self.tmp   # Write

action Init:
    counter = 0
    threads = []
    for _ in range(NUM_THREADS):
        threads.append(Thread())

always eventually assertion CounterEqualsNumThreads:
    return counter == NUM_THREADS

transition assertion Monotonic(before, after):
    return before.counter <= after.counter

```

To fix this this, you can mark the read-modify-write atomically.

```
---
# Disable deadlock detection, in this model, it is expected after all the threads complete, no further action needed
deadlock_detection: false 
options:
    # Since we are modelling thread behavior in single system, let's disable crash fault injection
    crash_on_yield: false

---

NUM_THREADS = 3 # Reduced for playground

role Thread:
    action Init:
        self.tmp = 0
        self.status = "init"

    fair action Run:
        require self.status == "init"
        self.status = "started"
        atomic:
            self.tmp = counter  # Read
            self.tmp += 1       # Increment
            counter = self.tmp   # Write

action Init:
    counter = 0
    threads = []
    for _ in range(NUM_THREADS):
        threads.append(Thread())

always eventually assertion CounterEqualsNumThreads:
    return counter == NUM_THREADS

transition assertion Monotonic(before, after):
    return before.counter <= after.counter

```

