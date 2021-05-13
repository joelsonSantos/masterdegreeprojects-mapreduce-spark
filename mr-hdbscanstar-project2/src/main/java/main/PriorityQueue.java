package main;

import java.io.Serializable;

public class PriorityQueue implements Serializable {

	private static final long serialVersionUID = 1L;
	private int[] elements;
	private int sizeHeap;

	public PriorityQueue(int[] elements) {
		this.elements = elements;
		this.sizeHeap = elements.length;
	}

	public void constructMinHeap() {
		for (int i = (this.elements.length / 2) - 1; i >= 0; i--) {
			// left and right
			if (this.elements.length == 2 * i + 2) {
				int aux = this.elements[i];
				if (this.elements[i] > this.elements[2 * i + 1]) {
					this.elements[i] = this.elements[2 * i + 1];
					this.elements[2 * i + 1] = aux;
					// decreaseKey(2 * i + 1);
				}
			} else {
				if ((this.elements[i] > this.elements[2 * i + 1]) || (this.elements[i] > this.elements[2 * i + 2])) {
					int aux = this.elements[i];
					// left greater than right child node.
					if (this.elements[2 * i + 1] < this.elements[2 * i + 2]) {
						this.elements[i] = this.elements[2 * i + 1];
						this.elements[2 * i + 1] = aux;
						decreaseKey(2 * i + 1);
					} else {
						this.elements[i] = this.elements[2 * i + 2];
						this.elements[2 * i + 2] = aux;
						decreaseKey(2 * i + 2);
					}
				}
			}
		}
	}

	public void decreaseKey(int i) {
		if (i == this.getSizeHeap()) {
			return;
		}
		if (this.getSizeHeap() == 2) {
			if (this.elements[0] > this.elements[1]) {
				int aux = this.elements[0];
				this.elements[0] = this.elements[1];
				this.elements[1] = aux;
				return;
			}
		}
		if ((this.getSizeHeap() <= 2 * i + 1) || (this.getSizeHeap() == 2 * i + 2)) {
			return;
		}
		if ((this.elements[i] > this.elements[2 * i + 1]) || (this.elements[i] > this.elements[2 * i + 2])) {
			int aux = this.elements[i];
			if (this.elements[2 * i + 1] < this.elements[2 * i + 2]) {
				this.elements[i] = this.elements[2 * i + 1];
				this.elements[2 * i + 1] = aux;
				decreaseKey(2 * i + 1);
			} else {
				this.elements[i] = this.elements[2 * i + 2];
				this.elements[2 * i + 2] = aux;
				decreaseKey(2 * i + 2);
			}
		}
	}

	public int extractMin() {
		int removed = this.elements[0];
		int aux = this.elements[this.getSizeHeap() - 1];
		this.elements[this.getSizeHeap() - 1] = this.elements[0];
		this.elements[0] = aux;
		this.sizeHeap--;
		decreaseKey(0);
		return removed;
	}

	public boolean isEmpty() {
		if (this.sizeHeap == 0) {
			return true;
		}
		return false;
	}

	public void setSizeHeap(int size) {
		this.sizeHeap = size;
	}

	public int getSizeHeap() {
		return this.sizeHeap;
	}

	public int[] getElements() {
		return this.elements;
	}
}