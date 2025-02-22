using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server;
using Voron.Debugging;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.PostingLists
{
    public sealed unsafe class PostingList : IDisposable
    {
        public Slice Name;
        private readonly LowLevelTransaction _llt;
        private PostingListState _state;
        private UnmanagedSpan<PostingListCursorState> _stk;
        private int _pos = -1, _len;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _scope;

        public PostingListState State => _state;
        internal LowLevelTransaction Llt => _llt;

        private NativeIntegersList _additions, _removals;

        public PostingList(LowLevelTransaction llt, Slice name, in PostingListState state)
        {
            if (state.RootObjectType != RootObjectType.Set)
                throw new InvalidOperationException($"Tried to open {name} as a set, but it is actually a " +
                                                    state.RootObjectType);
            Name = name;
            _llt = llt;
            _state = state;

            // PERF: We dont have the ability to dispose Set (because of how it is used) therefore,
            // we will just discard the memory as reclaiming it may be even more costly.  
            _scope = llt.Allocator.AllocateDirect(8 * sizeof(PostingListCursorState), out ByteString buffer);
            _stk = new UnmanagedSpan<PostingListCursorState>(buffer.Ptr, buffer.Size);
            if (llt.Flags == TransactionFlags.ReadWrite)
            {
                _additions = new NativeIntegersList(llt.Allocator);
                _removals = new NativeIntegersList(llt.Allocator);
            }
        }

        public static void Create(LowLevelTransaction tx, ref PostingListState state)
        {
            var newPage = tx.AllocatePage(1);
            PostingListLeafPage postingListLeafPage = new PostingListLeafPage(newPage);
            PostingListLeafPage.InitLeaf(postingListLeafPage.Header, 0);
            state.RootObjectType = RootObjectType.Set;
            state.Depth = 1;
            state.BranchPages = 0;
            state.LeafPages = 1;
            state.RootPage = newPage.PageNumber;
        }

        public List<long> DumpAllValues()
        {
            var iterator = Iterate();
            Span<long> buffer = stackalloc long[1024];
            var results = new List<long>();
            while (iterator.Fill(buffer, out var read) && read != 0)
            {
                results.AddRange(buffer[0..read].ToArray());
            }

            return results;
        }

        public void Add(long value)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), "Only positive values are allowed"); 
            
            _additions.Add(value);
        }

        public void Remove(ReadOnlySpan<long> values)
        {
            _removals.Add(values);
        }

        public void Add(ReadOnlySpan<long> values)
        {
            _additions.Add(values);
        }
        
        public void Remove(long value)
        {
            _removals.Add(value);
        }


        [Conditional("DEBUG")]
        public void Render()
        {
            DebugStuff.RenderAndShow(this);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void ResizeCursorState()
        {
            _llt.Allocator.Allocate(_stk.Length * 2 * sizeof(PostingListCursorState), out ByteString buffer);
            var newStk = new UnmanagedSpan<PostingListCursorState>(buffer.Ptr, buffer.Size);
            _stk.ToReadOnlySpan().CopyTo(newStk.ToSpan());
            _stk = newStk;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PostingListCursorState* FindSmallestValue()
        {
            _pos = -1;
            _len = 0;
            PushPage(_state.RootPage);

            var state = _stk.GetAsPtr(_pos);
            while (state->IsLeaf == false)
            {
                var branch = new PostingListBranchPage(state->Page);

                // Until we hit a leaf, just take the left-most key and move on. 
                long nextPage = branch.GetPageByIndex(0);
                PushPage(nextPage);

                state = _stk.GetAsPtr(_pos);
            }

            return state;
        }

        private void FindPageFor(long value)
        {
            _pos = -1;
            _len = 0;
            PushPage(_state.RootPage);
            ref var state = ref _stk[_pos];

            while (state.IsLeaf == false)
            {
                var branch = new PostingListBranchPage(state.Page);
                (long nextPage, state.LastSearchPosition, state.LastMatch) = branch.SearchPage(value);

                PushPage(nextPage);

                state = ref _stk[_pos];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PopPage()
        {
            _stk[_pos--] = default;
            _len--;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PushPage(long nextPage)
        {
            if (_pos + 1 >= _stk.Length) //  should never actually happen
                ResizeCursorState();

            Page page = _llt.GetPage(nextPage);
            _pos++; 
            
            var state = _stk.GetAsPtr(_pos);
            state->Page = page;
            state->LastMatch = 0;
            state->LastSearchPosition = 0;

            _len++;
        }

        public Iterator Iterate()
        {
            if (_additions.Count != 0 || _removals.Count != 0)
                throw new NotSupportedException("The set was modified, cannot read from it until is was committed");
            return new Iterator(this);
        }

        public struct Iterator 
        {
            private readonly PostingList _parent;
            private PostingListLeafPage.Iterator _it;

            public long Current;

            public Iterator(PostingList parent)
            {
                _parent = parent;
                Current = default;

                // We need to find the long.MinValue therefore the fastest way is to always
                // take the left-most pointer on any branch node.
                var state = _parent.FindSmallestValue();

                var leafPage = new PostingListLeafPage(state->Page);
                _it = leafPage.GetIterator();
            }


            public bool Seek(long from = long.MinValue)
            {
                _parent.FindPageFor(from);
                ref var state = ref _parent._stk[_parent._pos];
                var leafPage = new PostingListLeafPage(state.Page);

                _it = leafPage.GetIterator();
                _it.Skip(from);
                return true;
            }

            public bool Fill(Span<long> matches, out int total, long pruneGreaterThanOptimization = long.MaxValue)
            {
                // We will try to fill.
                total = _it.TryFill(matches, pruneGreaterThanOptimization);
                          
                while(true)
                {
                    var tmp = matches.Slice(total);
                    _it.Fill(tmp, out var read, out bool hasPrunedResults,  pruneGreaterThanOptimization);                                                                                      

                    // We haven't read anything, but we are not getting a pruned result.
                    if (read == 0 && hasPrunedResults == false)
                    {
                        var parent = _parent;
                        if (parent._pos == 0)
                            break;

                        parent.PopPage();

                        var llt = parent._llt;

                        while (true)
                        {
                            ref var state = ref parent._stk[_parent._pos];
                            state.LastSearchPosition++;
                            Debug.Assert(state.IsLeaf == false);
                            if (state.LastSearchPosition >= state.BranchHeader->NumberOfEntries)
                            {
                                if (parent._pos == 0)
                                    break;

                                parent.PopPage();
                                continue;
                            }

                            var branch = new PostingListBranchPage(state.Page);
                            (_, long pageNum) = branch.GetByIndex(state.LastSearchPosition);
                            var page = llt.GetPage(pageNum);
                            var header = (PostingListLeafPageHeader*)page.Pointer;

                            parent.PushPage(pageNum);

                            if (header->SetFlags == ExtendedPageType.SetBranch)
                            {
                                // we'll increment on the next
                                parent._stk[parent._pos].LastSearchPosition = -1;
                                continue;
                            }
                            _it = new PostingListLeafPage(page).GetIterator();
                            break;
                        }
                    }                        

                    total += read;
                    if (total == matches.Length)
                        break; // We are done.  

                    // We have reached the end by prunning.
                    if (hasPrunedResults)
                        break; // We are done.
                }

                if (total != 0)
                    Current = matches[total - 1];

                return total != 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext()
            {
                if (_it.MoveNext(out Current))
                    return true;

                var parent = _parent;
                if (parent._pos == 0)
                    return false;

                parent.PopPage();
                
                var llt = parent._llt;

                var it = _it;
                bool result = false;
                while (true)
                {
                    ref var state = ref parent._stk[_parent._pos];
                    state.LastSearchPosition++;
                    Debug.Assert(state.IsLeaf == false);
                    if (state.LastSearchPosition >= state.BranchHeader->NumberOfEntries)
                    {
                        if (parent._pos == 0)
                            break;

                        parent.PopPage();
                        continue;
                    }

                    var branch = new PostingListBranchPage(state.Page);
                    (_, long pageNum) = branch.GetByIndex(state.LastSearchPosition);
                    var page = llt.GetPage(pageNum);
                    var header = (PostingListLeafPageHeader*)page.Pointer;

                    parent.PushPage(pageNum);

                    if (header->SetFlags == ExtendedPageType.SetBranch)
                    {
                        // we'll increment on the next
                        parent._stk[parent._pos].LastSearchPosition = -1;
                        continue;
                    }
                    it = new PostingListLeafPage(page).GetIterator();
                    if (it.MoveNext(out Current))
                    {
                        result = true;
                        break;
                    }
                }

                _it = it;
                return result;
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }
        }

        public List<long> AllPages()
        {
            var result = new List<long>();
            AddPage(_llt.GetPage(_state.RootPage));
            return result;

            void AddPage(Page p)
            {
                result.Add(p.PageNumber);
                var state = new PostingListCursorState { Page = p, };
                if (state.BranchHeader->SetFlags != ExtendedPageType.SetBranch)
                    return;
                
                var branch = new PostingListBranchPage(state.Page);
                foreach (var child in branch.GetAllChildPages())
                {
                    var childPage = _llt.GetPage(child);
                    AddPage(childPage);
                }
            }
        }

        public void Dispose()
        {
            _additions.Dispose();
            _removals.Dispose();
            _scope.Dispose();
        }

        public void PrepareForCommit()
        {
            Span<long> additions = _additions.Items;
            Span<long> removals = _removals.Items;
            additions.Sort();
            removals.Sort();

            while (additions.IsEmpty == false || removals.IsEmpty == false)
            {
                bool hadRemovals = removals.IsEmpty == false;
                
                long first = -1;
                if (additions.IsEmpty == false)
                    first = additions[0];
                if (removals.IsEmpty == false) 
                    first = first == -1 ? removals[0] : Math.Min(removals[0], first);
            
                FindPageFor(first);
                long limit = NextParentLimit();
                ref var state = ref _stk[_pos];
                state.Page = _llt.ModifyPage(state.Page.PageNumber);
                var leafPage = new PostingListLeafPage(state.Page);
                
                long firstBaseline = first & int.MinValue;
                if (state.LeafHeader->Baseline != firstBaseline) // wrong baseline, need new page
                {
                    if (state.LeafHeader->NumberOfCompressedRuns != 0)
                    {
                        var page = _llt.AllocatePage(1);
                        var newPage = new PostingListLeafPage(page);
                        PostingListLeafPage.InitLeaf(newPage.Header, firstBaseline);
                        AddToParentPage(firstBaseline, page.PageNumber);
                        continue;
                    }
                    leafPage.Header->Baseline = firstBaseline;
                }

                _state.NumberOfEntries -= leafPage.Header->NumberOfEntries;

                var extras = leafPage.Update(_llt, ref additions, ref removals, limit);
                _state.NumberOfEntries += leafPage.Header->NumberOfEntries;

                if (extras != null) // we overflow and need to split excess to additional pages
                {
                    AddNewPageForTheExtras(leafPage, extras);
                }
                else if (hadRemovals && _len > 1 && leafPage.SpaceUsed < Constants.Storage.PageSize / 2)
                {
                    // check if we want to merge this page
                    PopPage();
            
                    ref var parent = ref _stk[_pos];
            
                    var branch = new PostingListBranchPage(parent.Page);
                    Debug.Assert(branch.Header->NumberOfEntries >= 2);
                    var siblingIdx = GetSiblingIndex(parent);
                    var (_, siblingPageNum) = branch.GetByIndex(siblingIdx);

                    var siblingPage = _llt.GetPage(siblingPageNum);
                    var siblingHeader = (PostingListLeafPageHeader*)siblingPage.Pointer;
                    if (siblingHeader->SetFlags != ExtendedPageType.SetLeaf)
                        continue;
                    
                    if (siblingHeader->Baseline != leafPage.Header->Baseline)
                        continue; // we cannot merge pages from different leafs

                    var sibling = new PostingListLeafPage(siblingPage);
                    if (sibling.SpaceUsed + leafPage.SpaceUsed > Constants.Storage.PageSize / 2 + Constants.Storage.PageSize / 4)
                        continue; // if the two pages together will be bigger than 75%, can skip merging

                    
                    PostingListLeafPage.Merge(_llt, 
                        leafPage.Header,
                        parent.LastSearchPosition == 0 ? leafPage.Header : siblingHeader,
                        parent.LastSearchPosition == 0 ? siblingHeader : leafPage.Header
                        );

                    MergeSiblingsAtParent();
                } 
            }
        }
        
        private static int GetSiblingIndex(in PostingListCursorState parent)
        {
            return parent.LastSearchPosition == 0 ? 1 : parent.LastSearchPosition - 1;
        }
        
        private void MergeSiblingsAtParent()
        {
            ref var state = ref _stk[_pos];
            state.Page = _llt.ModifyPage(state.Page.PageNumber);
            
            var current = new PostingListBranchPage(state.Page);
            Debug.Assert(current.Header->SetFlags == ExtendedPageType.SetBranch);
            var (siblingKey, siblingPageNum) = current.GetByIndex(GetSiblingIndex(in state));
            var (leafKey, leafPageNum) = current.GetByIndex(state.LastSearchPosition);

            var siblingPageHeader = (PostingListLeafPageHeader*)_llt.GetPage(siblingPageNum).Pointer;
            if (siblingPageHeader->SetFlags == ExtendedPageType.SetBranch)
                _state.BranchPages--;
            else
                _state.LeafPages--;
            
            _llt.FreePage(siblingPageNum);
            current.Remove(siblingKey);
            current.Remove(leafKey);

            // if it is empty, can just replace with the child
            if (current.Header->NumberOfEntries == 0)
            {
                var leafPage = _llt.GetPage(leafPageNum);
                
                long cpy = state.Page.PageNumber;
                leafPage.CopyTo(state.Page);
                state.Page.PageNumber = cpy;

                if (_pos == 0)
                    _state.Depth--; // replaced the root page

                _state.BranchPages--;
                _llt.FreePage(leafPageNum);
                return;
            }

            var newKey = Math.Min(siblingKey, leafKey);
            if (current.TryAdd(_llt, newKey, leafPageNum) == false)
                throw new InvalidOperationException("We just removed two values to add one, should have enough space. This error should never happen");

            if (_pos == 0)
                return; // root has no siblings

            if (current.Header->NumberOfEntries > PostingListBranchPage.MinNumberOfValuesBeforeMerge)
                return;

            PopPage();
            ref var parent = ref _stk[_pos];
            
            var gp = new PostingListBranchPage(parent.Page);
            var siblingIdx = GetSiblingIndex(parent);
            (_, siblingPageNum) = gp.GetByIndex(siblingIdx);
            var siblingPage = _llt.GetPage(siblingPageNum);
            var siblingHeader = (PostingListLeafPageHeader*)siblingPage.Pointer;
            if (siblingHeader->SetFlags != ExtendedPageType.SetBranch)
                return;// cannot merge leaf & branch
            
            var sibling = new PostingListBranchPage(siblingPage);
            if (sibling.Header->NumberOfEntries + current.Header->NumberOfEntries > PostingListBranchPage.MinNumberOfValuesBeforeMerge * 2)
                return; // not enough space to _ensure_ that we can merge

            for (int i = 0; i < sibling.Header->NumberOfEntries; i++)
            {
                (long key, long page) = sibling.GetByIndex(i);
                if(current.TryAdd(_llt, key, page) == false)
                    throw new InvalidOperationException("Even though we have checked for spare capacity, we run out?! Should not hapen ever");
            }

            MergeSiblingsAtParent();
        }

        private void AddNewPageForTheExtras(PostingListLeafPage leafPage, List<PostingListLeafPage.ExtraSegmentDetails> extras)
        {
            int idx = 0;
            var page = _llt.AllocatePage(1);
            _state.LeafPages++;
            var newPage = new PostingListLeafPage(page);
            PostingListLeafPage.InitLeaf(newPage.Header, leafPage.Header->Baseline);
            long firstValue = leafPage.Header->Baseline | extras[0].FirstValue;
            while (idx < extras.Count)
            {
                var cur = extras[idx];
                if (PostingListLeafPage.TryAdd(newPage.Header, cur.Compressed.Span))
                {
                    cur.Scope.Dispose();
                    idx++;
                    continue;
                }
                _state.NumberOfEntries += newPage.Header->NumberOfEntries;
                AddToParentPage(firstValue, newPage.Header->PageNumber);
                page = _llt.AllocatePage(1);
                newPage = new PostingListLeafPage(page);
                firstValue = leafPage.Header->Baseline | cur.FirstValue;
                PostingListLeafPage.InitLeaf(newPage.Header, leafPage.Header->Baseline);
            }

            _state.NumberOfEntries += newPage.Header->NumberOfEntries;
            
            AddToParentPage(firstValue, newPage.Header->PageNumber);
        }

        private long NextParentLimit()
        {
            var cur = _pos;
            while (cur > 0)
            {
                ref var state = ref _stk[cur - 1];
                if (state.LastSearchPosition + 1 < state.BranchHeader->NumberOfEntries)
                {
                    var (key, _) = new PostingListBranchPage(state.Page).GetByIndex(state.LastSearchPosition + 1);
                    return key;
                }
                cur--;
            }
            return long.MaxValue;
        }
        
        private void AddToParentPage(long separator, long newPage)
        {
            if (_pos == 0) // need to create a root page
            {
                CreateRootPage();
            }

            PopPage();
            ref var state = ref _stk[_pos];
            state.Page = _llt.ModifyPage(state.Page.PageNumber);
            var parent = new PostingListBranchPage(state.Page);
            if (parent.TryAdd(_llt, separator, newPage))
                return;

            SplitBranchPage(separator, newPage);
        }

        private void SplitBranchPage(long key, long value)
        {
            ref var state = ref _stk[_pos];

            var pageToSplit = new PostingListBranchPage(state.Page);
            var page = _llt.AllocatePage(1);
            var branch = new PostingListBranchPage(page);
            branch.Init();
            _state.BranchPages++;
            
            // grow rightward
            if (key > pageToSplit.Last)
            {
                if (branch.TryAdd(_llt, key, value) == false)
                    throw new InvalidOperationException("Failed to add to a newly created page? Should never happen");
                AddToParentPage(key, page.PageNumber);
                return;
            }

            // grow leftward
            if (key < pageToSplit.First)
            {
                long oldFirst = pageToSplit.First;
                var cpy = page.PageNumber;
                state.Page.AsSpan().CopyTo(page.AsSpan());
                page.PageNumber = cpy;

                cpy = state.Page.PageNumber;
                state.Page.AsSpan().Clear();
                state.Page.PageNumber = cpy;

                var curPage = new PostingListBranchPage(state.Page);
                curPage.Init();
                if(curPage.TryAdd(_llt, key, value) == false)
                    throw new InvalidOperationException("Failed to add to a newly initialized page? Should never happen");
                AddToParentPage(oldFirst, page.PageNumber);
                return;
            }

            // split in half
            for (int i = pageToSplit.Header->NumberOfEntries / 2; i < pageToSplit.Header->NumberOfEntries; i++)
            {
                var (k, v) = pageToSplit.GetByIndex(i);
                if(branch.TryAdd(_llt, k, v) == false)
                    throw new InvalidOperationException("Failed to add half our capacity to a newly created page? Should never happen");
            }

            pageToSplit.Header->NumberOfEntries /= 2;// truncate entries
            var success = pageToSplit.Last > key ?
                branch.TryAdd(_llt, key, value) :
                pageToSplit.TryAdd(_llt, key, value);
            if(success == false)
                throw new InvalidOperationException("Failed to add final to a newly created page after adding half the capacit? Should never happen");

            AddToParentPage(branch.First, page.PageNumber);
        }
        
        private void InsertToStack(PostingListCursorState newPageState)
        {
            // insert entry and shift other elements
            if (_len + 1 >= _stk.Length) // should never happen
                ResizeCursorState();

            var src = _stk.ToReadOnlySpan().Slice(_pos + 1, _len - (_pos + 1));
            var dest = _stk.ToSpan().Slice(_pos + 2);
            src.CopyTo(dest);

            _len++;
            _stk[_pos + 1] = newPageState;
            _pos++;
        }

        private void CreateRootPage()
        {
            _state.Depth++;
            _state.BranchPages++;
            // we'll copy the current page and reuse it, to avoid changing the root page number
            var page = _llt.AllocatePage(1);
            long cpy = page.PageNumber;
            ref var state = ref _stk[_pos];
            Memory.Copy(page.Pointer, state.Page.Pointer, Constants.Storage.PageSize);
            page.PageNumber = cpy;
            Memory.Set(state.Page.DataPointer, 0, Constants.Storage.PageSize - PageHeader.SizeOf);
            var rootPage = new PostingListBranchPage(state.Page);
            rootPage.Init();
            rootPage.TryAdd(_llt, long.MinValue, cpy);

            InsertToStack(new PostingListCursorState
            {
                Page = page,
                LastMatch = state.LastMatch,
                LastSearchPosition = state.LastSearchPosition
            });
            state.LastMatch = -1;
            state.LastSearchPosition = 0;
        }

        public static long Update(LowLevelTransaction transactionLowLevelTransaction, ref PostingListState postingListState, ReadOnlySpan<long> additions, ReadOnlySpan<long> removals)
        {
            using var pl = new PostingList(transactionLowLevelTransaction, Slices.Empty, postingListState);
            pl.Add(additions);
            pl.Remove(removals);
            pl.PrepareForCommit();
            postingListState = pl.State;

            return pl.State.NumberOfEntries;
        }
    }
}
